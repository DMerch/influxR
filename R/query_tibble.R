#' A reference class that maintains the output of the http stream parser
#'
#' Anytime httr2::req_stream() is called, it must be supplied the name of
#' a parser to parse the stream.  In order to maintain output information between
#' successive calls to the parser, a reference class object is maintained in
#' the parser function closure.
#'
#' @field tables ANY. The list of CSV tables returned after processing all data
#'   chunks. For most queries, only a single table is returned as the first
#'   element of the list.
#' @field error_message character. The contents of the http body when something
#'   other than a CSV table is received.
#' @field table_chunks ANY. The list of partial CSV tables that are used to form
#'   the current full CSV table being parsed across multiple chunks.
#' @field record_count integer. The total number of CSV records read across all
#'   tables.
#' @field parser_count integer. The number of times the parser was called.  This
#'   is equivalent to the number of http chunks received.
#' @field debug logical. A flag indicating that the parser should output
#'   debug messages.
#'
#' @importFrom methods new
#'
#' @return An instance of a InfluxParserOutput object
#'
InfluxParserOutput <- setRefClass(
  "InfluxParserOutput",
  fields = list(
    tables = "ANY",
    error_message = "character",
    table_chunks = "ANY",
    record_count = "integer",
    parser_count = "integer",
    debug = "logical"
  ),
  methods = list(
    initialize = function (debug = FALSE) {
      tables <<- list()
      error_message <<- ""
      table_chunks <<- list()
      record_count <<- 0L
      parser_count <<- 0L
      debug <<- debug
    }
  )
)

#' Convert the datatype annotation of the influx CSV table into
#' readr::read_csv() datatypes
#'
#' The first column is always ignored.
#'
#' @param header character. A comma separated list of column data types
#' as returned by influx's annotated CSV output.
#'
#' @return A column type character string as expected by readr::read_csv()
#'
map_influx_datatypes <- function (header) {
  influx_types <- stringi::stri_split_fixed(header, ",")[[1]]
  influx_types <- dplyr::if_else(influx_types == "", "ignore", influx_types)

  paste(
    dplyr::recode(
      influx_types,
      "base64Binary" = "c",
      "boolean" = "c",
      "dateTime:number" = "d",
      "dateTime:RFC3339" = "T",
      "dateTime:RFC3339Nano" = "T",
      "double" = "d",
      "duration" = "d",
      "long" = "d",
      "ignore" = "_",
      "string" = "c",
      "unsignedLong" = "d",
      "#datatype" = "_",
      default = "?"
    ), collapse = "")
}

#' Generate a influx stream parser that can be assigned to the callback argument
#' of httr2::req_stream()
#'
#' httr2::req_stream() requires a function that takes a single argument
#' which is the raw bytes returned from the stream.  Because the parser state
#' must be maintained by parser, this parser is created as a closure.
#'
#' @param output A InfluxParserOutput object that is used by the parser to pass
#'   information back to the function that called httr2::req_stream().
#'
#' @return A function that takes a single argument containing the raw data read
#'   from the stream.
#'
generate_influx_response_parser <- function (output) {

  # The following variables are part of the closure object returned. They
  # maintain the parser state between successive calls.
  #
  # has_partial_line - boolean flag that when TRUE indicates that the last
  # data buffer ended in the middle of a line instead of at the end of a
  # line
  #
  # last_line - the contents of the last partial line read from the previous
  # buffer.
  #
  # col_names - the names to use for the columns.  Following the convention of
  # readr::read_csv, its value is 0 when a header is expected and a list of
  # column names when a header is not expected.
  #
  # col_types - the data types expected for each column.  The influx server is
  # configured as part of the query to return the datatypes as part of the
  # annotations listed before the raw CSV table.
  #
  has_partial_line <- FALSE
  last_line <- ""
  col_names <- TRUE
  col_types <- "_?" # default if no annotation given

  parser <- function (x) {

    output$parser_count <- output$parser_count + 1L
    data <- rawToChar(x)

    # Determine the response type if this is the first chunk of data
    if (output$parser_count == 1L) {

      # Queries that return no data return a carriage return and newline
      if (data == "\r\n") {
        output$tables <- list(tibble::tibble())
        return(TRUE)
      }

      # Malformed queries return a JSON error message
      if (stringi::stri_startswith_fixed(data, "{")) {
        output$error_message <- data
        return(TRUE)
      }

      # Otherwise, should be annotated CSV
    }

    # If the previous chunk ended in the middle of a line, prepend the partial
    # line to the beginning of this chunk
    if (has_partial_line) {
      data <- paste0(last_line, data)
    }

    # If the current chunk ends in the middle of a line, save the partial last
    # line to be processed with the next chunk.
    data_length <- nchar(data)
    has_partial_line <<- (substr(data, data_length, data_length) != "\n")
    if (has_partial_line) {
      eol_index <- stringi::stri_locate_last(data, fixed = "\n")[[1]]
      if (is.na(eol_index)) {
        last_line <<- data
        return(TRUE)
      }
      chunk <- stringi::stri_sub(data, 1, eol_index)
      last_line <<- stringi::stri_sub(data, eol_index + 1, data_length)
    } else {
      chunk <- data
      last_line <<- ""
    }

    # Remove the data variable since it is no longer needed. This may allow some
    # memory to be garbage collected if the data was broken into two pieces?
    rm(data)

    # Each table ends with a carriage return, newline, carriage return, newline
    # sequence
    csv_chunks <- stringi::stri_split_fixed(chunk, "\r\n\r\n")[[1]]
    num_csv_chunks <- length(csv_chunks)

    # Remove the chunk variable since it is no longer needed. This may allow
    # some memory to be garbage collected if the data was broken into multiple?
    rm(chunk)

    # Parse each separate csv data set
    for(csv_chunk_index in 1:num_csv_chunks) {

      csv_chunk <- csv_chunks[[csv_chunk_index]]

      # Ignore the end of buffer immediately after the table ends.
      if (csv_chunk == "") {
        next
      }

      # Because the end of table sequence could have been split into two
      # csv_chunks, we must make sure we recognize the end of the table, not try
      # to parse the extra carriage return newline, and finalize the current
      # table.
      if (csv_chunk == "\r\n") {
        output$tables <-
          append(output$tables, list(do.call(
            dplyr::bind_rows, output$table_chunks
          )))
        output$table_chunks = list()
        col_names <<- TRUE
        col_types <<- "_?"
        next
      }

      # Parse the data type for each column from the annotation.  Remove the
      # datatype row from the data so that it is not assumed to be a header
      # row.
      if (isTRUE(col_names) &&
          stringi::stri_startswith_fixed(csv_chunk, "#datatype")) {
        chunk_length <- nchar(csv_chunk)
        eol_index <- stringi::stri_locate_first(csv_chunk, fixed = "\r\n")[[1]]
        datatypes_header <- stringi::stri_sub(csv_chunk, 1, eol_index - 1)
        csv_chunk <- stringi::stri_sub(csv_chunk, eol_index + 2, chunk_length)
        col_types <<- map_influx_datatypes(datatypes_header)
      }

      # If the last line of the CSV table is the only line in the csv_chunk
      # (after being split from the original larger chunk), then we have to add
      # a newline so that read::read_csv knows it is processing a string instead
      # of a file name.
      if (isFALSE(stringi::stri_detect_fixed(csv_chunk, "\n"))) {
        csv_chunk <- paste0(csv_chunk, "\n")
      }

      # Read the CSV data from the current chunk.
      dataframe_chunk <- readr::read_csv(
        csv_chunk,
        col_types = col_types,
        col_names = col_names,
        na = "null",
        progress = FALSE,
        name_repair = "minimal" # don't warn about empty column
      )

      # If the previous CSV output contained column names, use those column names
      # for any remaining chunks that are part of the current CSV table.
      if (isTRUE(col_names)) {
        col_names <<- names(dataframe_chunk)
        if (isTRUE(output$debug)) {
          cat("Found new CSV table\n")
          cat("Column names :", paste(col_names, collapse = ","), "\n")
          cat("Column types :", col_types, "\n\n")
        }
      }

      # Save the current parsed CSV data so it can be combined with all
      # the partial CSV tables that make up the final table.
      output$record_count <- output$record_count + nrow(dataframe_chunk)
      output$table_chunks <- append(output$table_chunks, list(dataframe_chunk))

      # Finalize tables for which complete data has been received. The
      # stri_split function used to identify the table end points returns an
      # extra empty string after the end of table character sequence that is at
      # the end of the buffer.  This means that table ends can be identified
      # anytime there are more csv chunks than the csv_chunk_index counter.
      if (csv_chunk_index < num_csv_chunks) {
        output$tables <-
          append(output$tables, list(do.call(
            dplyr::bind_rows, output$table_chunks
          )))
        output$table_chunks = list()
        col_names <<- TRUE
        col_types <<- "_?"
      }

    }

    return(TRUE)
  }

  return(parser)
}

#' Send a query command to an influx server and parse the results as a list of
#' tibbles
#'
#' If the http command returns an error, an error is thrown.
#'
#' @param con influx database connection returned by \code{connect}
#' @param code a string containing the flux code to run as the query
#' @param flatten a boolean indicating that a single tibble should be returned instead
#' of a list when only a single table is returned.
#' @param max_tries numeric indicating how many times the request should be attempted
#' @param debug print verbose messages to standard output
#'
#' @return A tibble for each CSV table returned.
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect(url = "http://myserver.mycompany.com",
#'                org = "my-org",
#'                token = "my-token")
#' code <- '
#' import "influxdata/influxdb/schema"
#' schema.tagKeys(
#'   bucket: "my-bucket",
#'   predicate: (r) => true,
#'   start: 2021-03-01-T00:00:00Z)
#' '
#' df <- query_tibble(con, code)
#' }
query_tibble <- function (con, code, flatten = TRUE, max_tries = 1, debug = FALSE) {
  check_influxdb_con(con)
  stopifnot(is.character(code))

  # Build the message to execute the query
  req <-
    con$base_request %>%
    httr2::req_url_path_append(api_path("query")) %>%
    httr2::req_url_query(org = con$org) %>%
    httr2::req_headers(`Accept-Encoding` = "gzip",
                       `Content-Type` = "application/json") %>%
    httr2::req_body_json(list(
      query = code,
      dialect = list(
        header = TRUE,
        delimeter = ",",
        annotations = list("datatype"),
        commentPrefix = "#",
        dateTimeFormat = "RFC3339"
      )
    ))

  # Show the posted message if debug is enabled
  if (isTRUE(debug)) {
    cat("\n")
    print(httr2::req_dry_run(req))
  }

  # Run the query and measure how long it takes.  Results from the query are stored
  # in the "results" reference class object.
  start <- Sys.time()
  attempt <- 1
  is_invalid <- TRUE
  while(attempt <= max_tries && is_invalid) {
    # Build reference class data structure that maintains the data parsed
    # from the chunked http data stream
    results <- InfluxParserOutput$new(debug = debug)
    resp <- httr2::req_stream(req,
                              callback = generate_influx_response_parser(results),
                              buffer_kb = 512)
    is_invalid <- httr2::resp_is_error(resp)
    attempt <- attempt + 1
  }
  stop <- Sys.time()

  # Display the response if debug is enabled
  if (isTRUE(debug)) {
    print(resp)
    cat("\nRecords read = ", results$record_count, "\n")
    print(stop - start)
    cat("\n\n")
  }

  # If the an http error message was return, throw an error
  if (httr2::resp_is_error(resp)) {
    stop(
      sprintf(
        "query_tibble failed [HTTP status %d]\n%s\n",
        httr2::resp_status(resp),
        results$error_message
      ),
      call. = FALSE
    )
  }

  # Return the list of tables or a single table if flatten is TRUE
  if (isTRUE(flatten) && length(results$tables) == 1) {
    return_value <- results$tables[[1]]
  } else {
    return_value <- results$tables
  }

  return(return_value)

}
