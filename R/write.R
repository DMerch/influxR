#' Write a dataframe to a influx connection
#'
#' @param con the influx database endpoint created by \code{connect}
#' @param data a dataframe containing the data to write
#' @param bucket The name of the bucket to write the data to.
#' @param measurement the name of the influx measurement to use.  Cannot be specified if
#' \code{measurement_col} is also specified.
#' @param measurement_col the name of the column in the data frame that contains the measurement
#' name to use for each row of data.  Cannot be specified if \code{measurement} is also specified.
#' @param time_col The name of the column containing the timepoint for each row.
#' @param tag_cols The names of the columns that are to be treated as tags by influx
#' @param field_cols The names of the columns that are to be treated as fields by influx
#' @param batch_size The number of lines to write per HTTP POST.  5000 is the optimal number for
#' influx line protocol writes.
#' @param precision The precision of the data to write.
#'
#' @return Influx V2 API \code{httr::POST} response of the last batch of data that was written
#'
#' @export
#'
write <-
  function (con,
            data,
            bucket,
            measurement = NULL,
            measurement_col = NULL,
            time_col = "time",
            tag_cols = NULL,
            field_cols = NULL,
            batch_size = 5000,
            precision = "s") {
    check_influxdb_con(con)
    check_char(bucket)
    check_char_or_NULL(measurement)
    check_char_or_NULL(measurement_col)

        # validate arguments
    column_names <- names(data)

    if (is.character(measurement_col) && isFALSE(measurement_col %in% column_names)) {
      stop(sprintf("measurement_col '%s' does not exist in the data frame"), measurement_col)
    }

    if (!is.character(measurement) && !is.character(measurement_col)) {
      stop("Either measurement or measurement_col must be specified")
    }

    if (isFALSE(time_col %in% column_names)) {
      stop(sprintf("time_col '%s' does not exist in data frame", time_col))
    }
    if (isFALSE(lubridate::is.timepoint(dplyr::pull(data, time_col)))) {
      stop(sprintf("time_col '%s' is not a timepoint", time_col))
    }

    bad_tag <- purrr::detect(tag_cols, ~ !(.x %in% column_names))
    if (is.character(bad_tag)) {
      stop(sprintf("tag column '%s' does not exist in the data frame"), bad_tag)
    }

    if (is.null(field_cols)) {
      field_cols <- dplyr::setdiff(column_names, c(measurement_col, time_col, tag_cols))
    } else {
      bad_field <- purrr::detect(field_cols, ~ !(.x %in% column_names))
      if (is.character(bad_field)) {
        stop(sprintf("field column '%s' does not exist in the data frame"),
             bad_field)
      }
    }

    # write the data in chunks
    total_rows <- nrow(data)
    for (x in seq(1, ceiling(total_rows / batch_size))) {
      begin <- (x - 1) * batch_size + 1
      end <- (x * batch_size)
      if (end > total_rows) {
        end <- total_rows
      }
      chunk <- data[begin:end, ]
      body <- convert_to_line_protocol(
        data = chunk,
        measurement = measurement,
        measurement_col = measurement_col,
        time_col = time_col,
        tag_cols = tag_cols,
        field_cols = field_cols
      )
      resp <- post_line_protocol(con, bucket = bucket, precision = precision, data = body)
      # if (httr::http_error(resp)) {
      #   stop(
      #     sprintf(
      #       "Failed to write data [HTTP status code %s]\n%s",
      #       httr::status_code(resp),
      #       httr::content(resp)
      #     ),
      #     call. = FALSE
      #   )
      #   return(resp)
      # }
    }

    resp
  }

#' Convert a dataframe to line protocol
#'
#' @param data the dataframe containing the data to convert to line protocol format
#' @param measurement the name of the influx measurement to use.  Cannot be specified if
#' \code{measurement_col} is also specified.
#' @param measurement_col the name of the column in the data frame that contains the measurement
#' name to use for each row of data.  Cannot be specified if \code{measurement} is also specified.
#' @param time_col The name of the column containing the timepoint for each row.
#' @param tag_cols The names of the columns that are to be treated as tags by influx
#' @param field_cols The names of the columns that are to be treated as fields by influx
#'
#' @export
#'
#' @return character string of line protocol lines
#'
convert_to_line_protocol <-
  function (data,
            measurement = NULL,
            measurement_col = NULL,
            time_col,
            tag_cols,
            field_cols = NULL) {

    if (is.character(measurement_col)) {
      measurement_data <- quote_specials(dplyr::pull(data, measurement_col), measurement)
    } else {
      measurement_data <- quote_specials(measurement, "measurement")
    }

    tags_data <- purrr::map(sort(tag_cols), function (name) {
      column_data <- dplyr::pull(data, name)
      dplyr::if_else(is.na(column_data),
                     # then
                     "",
                     # else
                     paste0(
                       ",",
                       quote_specials(name, purpose = "tag_key"),
                       "=",
                       quote_specials(column_data, purpose = "tag_value")
                     ))
    })

    field_separator <- " "
    fields_data <- purrr::map(sort(field_cols), function (name) {
      column_data <- dplyr::pull(data, name)
      formatted_field <-
        dplyr::if_else(
          is.na(column_data),
          # then
          "",
          # else
          paste0(
            field_separator,
            quote_specials(name, purpose = "field_key"),
            "=",
            quote_specials(column_data, purpose = "field_value")
          )
        )
      field_separator <<- ","
      formatted_field
    })


    time_data <- paste0(" ", as.integer(dplyr::pull(data, time_col)))

    body <-
      do.call(paste0, c(
        list(measurement_data),
        tags_data,
        fields_data,
        list(time_data),
        sep = "\n",
        collapse = ""
      ))

    body
}

#' Post the line protocol formatted data to the connection
#'
#' @param con the influx database endpoint created by \code{connect}
#' @param bucket the name of the influx bucket to write the data to
#' @param data The data (string or character vector) to be posted as "text/plain; charset=utf-8"
#' @param precision Precision of the time point being written.  This value is any valid
#' influx precision value.
#'
#' @return Influx V2 API \code{httr::POST} response
#'
post_line_protocol <- function (con, bucket, data, precision) {

  req <- request(
    con,
    path = api_path("write"),
    query = list(
      bucket = bucket,
      org = con$org,
      precision = precision
    ),
    method = "POST"
  ) %>%
    httr2::req_body_raw(data) %>%
    httr2::req_headers(`Context-Type` = "text/plain; charset=utf-8")

  if (getOption("influxr.debug", FALSE) == TRUE) {
    print(httr2::req_dry_run(req))
  }
  resp <- httr2::req_perform(req)

  return(resp)

}

#' Quote characters following influx quoting rules for line protocol items
#'
#' @param vec vector of data to quote each element of
#' @param purpose the purpose of the vector of data in the line protocol output.  This purpose
#' defines the influx quoting rules used.
#'
#' @return vector data converted to character strings and quoted according to its purpose
#' @export
#'
#' @examples
#' quote_specials("example 1", purpose = "measurement")
#' quote_specials(c("name=1", "name2", "name,3"), purpose = "tag_key")
quote_specials <-
  function (vec, purpose = c("measurement", "tag_key", "tag_value", "field_key", "field_value")) {

    purpose <- match.arg(purpose)
    regexp <- switch(
      purpose,
      "measurement" = "([, ])",
      "tag_key"     = "([,= ])",
      "tag_value"   = "([,= ])",
      "field_key"   = "([,= ])",
      "field_value" = '(["\\\\])'
    )

    if (is.character(vec)) {
      text <-stringr::str_replace_all(vec, regexp, "\\\\\\1")
      if (purpose == "field_value") {
        text <- paste0('"', text, '"')
      }
    } else {
      text <- as.character(vec)
      if (is.integer(vec)) {
        text <- paste0(text, "i")
      }
    }

    text
  }
