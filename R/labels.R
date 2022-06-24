labels_path <- function (...) {
  api_path("labels", ...)
}

#' Generate label properties from a vector/list
#'
#' Label properties are stored in influx as strings.  Therefore, all elements in the
#' \code{values} argument should be of length 1 and should be capable of being coerced to a
#' string if not already a string.
#'
#' @param values Either a named vector or named list of property values
#'
#' @return A data structure in the format required by \code{create_label}
#' @export
#'
#' @examples
#' label_properties(list(p1 = 5, p2 = "ok", p3 = 1))
label_properties <- function (values) {

  # Ensure elements are named and of length 1
  if (!is.vector(values) ||
      is.null(names(values)) ||
      !purrr::every(names(values), function (x) x != "") ||
      !purrr::every(values, function (x) length(x) == 1)) {
    stop("Label properties should be a named vector or named list of single values")
  }

  # Coerce data type to characters if not already characters.
  # Influx only supports string valued properties
  lapply(as.list(values), as.character)
}

#' Get the list of labels available at the influx connection
#'
#' @param con influxdb connection previously established with \code{connect}
#'
#' @return influxdb V2 API json response
#' @export
#'
list_labels <- function (con) {
  check_influxdb_con(con)
  respond_with_json(con, path = labels_path())
}

#' Create a label on the server running at the influx connection
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param name name of the label
#' @param properties Key value pairs to store in the label.  Should be the output
#' of the \code{label_properties} function
#' @param orgID ID of organization the label is scoped to.  Defaults to orgID of \code{con}
#'
#' @return influxdb V2 API json response
#'
#' @export
#'
#' @examples
#' \dontrun{
#' create_label(con, name = "label_name", properties = label_properties(list(a = 1, b = 2)))
#' }
create_label <-
  function (con,
            name,
            properties = NULL,
            orgID = NULL) {
    check_influxdb_con(con)
    check_char(name)
    check_char_or_NULL(orgID)
    if (is.null(orgID)) {
      orgID <- con$orgID
    }
    body <-
      drop_nulls(name = name,
                 orgID = orgID,
                 properties = properties)

    respond_to_json_with_json(con = con,
                              data = body,
                              path = labels_path())
  }

#' Delete a label from the influx database
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param labelID labelID of the label to delete
#'
#' @return influxdb V2 API json response
#' @export
#'
delete_label <- function (con, labelID) {
  check_influxdb_con(con)
  check_char(labelID)
  respond(con = con, path = labels_path(labelID))
}

#' Retrieve a label by labelID from the influx database
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param labelID the ID of the label to retrieve
#'
#' @return influxdb V2 API json response
#' @export
#'
retrieve_label <- function (con, labelID) {
  check_influxdb_con(con)
  check_char(labelID)
  respond_with_json(con, path = labels_path(labelID))
}

#' Updated a label on the server running at the influx connection
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param labelID ID of the label to update
#' @param name name of the label
#' @param properties Key value pairs to store in the label.  Should be the output
#' of the \code{label_properties} function
#'
#' @return influxdb V2 API json response
#'
#' @export
#'
update_label <-
  function (con,
            labelID,
            name = NULL,
            properties = NULL) {
    check_influxdb_con(con)
    check_char(labelID)
    check_char_or_NULL(name)
    check_char_or_NULL(properties)
    body <- drop_nulls(name = name, properties = properties)
    respond_to_json_with_json(
      con = con,
      data = body,
      path = labels_path(labelID),
      method = "PATCH"
    )
  }
