# This file is untested

var_path <- function (...) {
  api_path("variables", ...)
}

#' Get the list of variables available at the influx connection
#'
#' @param con the influx connection previously established with \code{connect}
#'
#' @return Influx V2 API json response
#' @export
#'
list_variables <- function (con) {
  check_influxdb_con(con)
  respond_with_json(con, path = var_path())
}

#' Create a variable on the server running at the influx connection
#'
#' @param con the influx connection previously established with \code{connect}
#' @param name name of the variable
#' @param arguments value of the variable.  It should be the output of either
#' \code{QueryVariableProperties}, \code{ConstantVariableProperties}, or
#' \code{MapVariableProperties}
#' @param orgID organization id the variable is to be associated with.  The default value is the
#' orgID from the connection.
#' @param createdAt date the parameter was created
#' @param description string description of the variable
#' @param labels labels Labels to create/attach to the variable. Should be the output of
#' the \code{variable_labels} function
#' @param selected default key to use if the variable store multiple key/value pairs
#' @param updatedAt date the parameter was last updated
#'
#' @return Influx V2 API json response
#'
#' @export
#'
#' @examples
#' \dontrun{
#' create_variable(tdb, name = "dims", arguments = MapVariableProperties(list(m=5, n=2)))
#' }
create_variable <- function (con,
                             name,
                             arguments,
                             orgID = NULL,
                             createdAt = NULL,
                             description = NULL,
                             labels = NULL,
                             selected = NULL,
                             updatedAt = NULL) {
  check_influxdb_con(con)
  check_char(name)
  stopifnot(is.list(arguments))
  check_char_or_NULL(orgID)
  if (is.null(orgID)) {
    orgID = con$orgID
  }
  check_char_or_NULL(createdAt)
  check_char_or_NULL(description)
  stopifnot(is.null(labels) || is.list(labels))
  stopifnot(is.null(selected) || is.list(selected))
  check_char_or_NULL(updatedAt)
  body <- drop_nulls(
    arguments = arguments,
    createdAt = createdAt,
    description = description,
    labels = labels,
    name = name,
    orgID = orgID,
    selected = selected,
    updatedAt = updatedAt
  )
  respond_to_json_with_json(
    con = con,
    data = body,
    path = var_path()
  )
}


#' Generate a list of labels to be associated with a variable
#'
#' @param values a named list of \code{label_properties}
#'
#' @return a data structure suitable as input to the \code{labels} argument of
#' \code{create_variable}
#'
#' @export
#'
#' @examples
#' labels <- list(l1 = label_properties(list(p1 = 1, p2 = 2, p3 = "3")),
#'                l2 = label_properties(list(p4 = "ok", p5 = "10")),
#'                l3 = label_properties(list(p6 = "bucket-name")))
#' variable_labels(labels)
variable_labels <- function (values) {
  purrr::map2(names(values), values, ~ list(name = .x, properties <- .y))
}


#' Generate a QueryVariableProperties variable from a query string
#'
#' @param query string query
#' @param language defaults to "flux"
#'
#' @return A data structure in the format required by \code{create_variable} to create
#' a QueryVariableProperties variable
#'
#' @export
#'
#' @examples
#'
#' QueryVariableProperties('schema.TagKeys("bucket-name")')
QueryVariableProperties <- function (query, language ="flux") {
  if (!is.character(query) || length(query) != 1) {
    stop("Query should be a single string value")
  }

  # TODO validate language argument

  list(type = "query", values = list(language = language, query = query))
}

#' Generate a ConstantVariableProperties variable type from a vector or list
#'
#' Constant variable properties are stored in influx as strings.  Therefore, all elements in the
#' \code{values} argument should be of length 1 and should be capable of being coerced to a
#' string if not already a string.
#'
#' @param values Either a vector or list of property values
#'
#' @return A data structure in the format required by \code{create_variable} to create
#' a MapVariableProperties variable
#'
#' @export
#'
#' @examples
#' ConstantVariableProperties(1)
#' ConstantVariableProperties("ok")
#' ConstantVariableProperties(c("value1", "value2"))
#' ConstantVariableProperties(list("value1", "value2"))
#'
ConstantVariableProperties <- function (values) {
  if (!is.vector(values) ||
      !purrr::every(values, function (x) length(x) == 1)) {
    stop("Map variable properties should be a named vector or named list of single values")
  }

  # Coerce data type to characters if not already characters.
  # Influx only supports string valued properties
  values <- purrr::map_chr(values, as.character)

  list(type = "constant", values = I(values))
}

#' Generate a MapVariableProperties variable type from a named vector or named list
#'
#' Map variable properties are stored in influx as strings.  Therefore, all elements in the
#' \code{values} argument should be of length 1 and should be capable of being coerced to a
#' string if not already a string.
#'
#' @param values Either a named vector or named list of property values
#'
#' @return A data structure in the format required by \code{create_variable} to create
#' a MapVariableProperties type variable
#'
#' @export
#'
#' @examples
#' MapVariableProperties(c(p1 = "v1", p2 = "v2", p3 = "v3"))
#' MapVariableProperties(list(p1 = "v1", p2 = 1.0, p3 = 5))
MapVariableProperties <- function (values) {

  # Ensure elements are named and of length 1
  if (!is.vector(values) ||
      is.null(names(values)) ||
      !purrr::every(names(values), function (x) x != "") ||
      !purrr::every(values, function (x) length(x) == 1)) {
    stop("Map variable properties should be a named vector or named list of single values")
  }

  # Coerce data type to characters if not already characters.
  # Influx only supports string valued properties
  values <- lapply(as.list(values), as.character)

  list(type = "map", values = values)
}

#' Delete a variable from the influx database
#'
#' @param con the influx connection previously established with \code{connect}
#' @param variableID influx variableID of the variable to delete
#'
#' @return Influx V2 API json response
#' @export
#'
delete_variable <- function (con, variableID) {
  check_influxdb_con(con)
  check_char(variableID)
  respond(con = con, path = var_path(variableID), method = "DELETE")
}


#' Retrieve a variable by variableID from the influx database
#'
#' @param con the influx connection previously established with \code{connect}
#' @param variableID the ID of the variable to retrieve
#'
#' @return Influx V2 API json response
#' @export
#'
retrieve_variable <- function (con, variableID) {
  check_influxdb_con(con)
  check_char(variableID)
  respond_with_json(con, path = var_path(variableID))
}

#' Update an influxdb variable
#'
#' @param con the database connection previously established with \code{connect}
#' @param variableID ID of the variable to update
#' @param name name of the variable
#' @param arguments value of the variable.  It should be the output of either
#' \code{query_variable_properties}, \code{constant_variable_properties}, or
#' \code{map_variable_properties}
#' @param createdAt date the parameter was created
#' @param description string description of the variable
#' @param labels labels Labels to create/attach to the variable. Should be the output of
#' the \code{variable_labels} function
#' @param selected default key to use if the variable store multiple key/value pairs
#' @param updatedAt date the parameter was last updated
#'
#' @return Influx V2 API json response
#'
#' @export
#'
update_variable <-
  function (con,
            variableID,
            name,
            arguments,
            createdAt = NULL,
            description = NULL,
            labels = NULL,
            selected = NULL,
            updatedAt = NULL) {

    check_influxdb_con(con)
    check_char(variableID)
    check_char_or_NULL(createdAt)
    check_char_or_NULL(description)
    stopifnot(is.list(labels))
    stopifnot(is.null(labels) || is.list(labels))
    stopifnot(is.null(selected) || is.list(selected))

    if (name %in% c(
      "and",
      "import",
      "not",
      "return",
      "option",
      "test",
      "empty",
      "in",
      "or",
      "package",
      "builtin"
    ) ||
    !stringr::str_detect(name, "^[_a-zA-Z]")) {
      stop("Illegal variable name - ", name)
    }

    body <-
      drop_nulls(
        name = name,
        arguments = arguments,
        orgID = con$orgID,
        createdAt = createdAt,
        description = description,
        labels = labels,
        selected = selected
      )

    respond_to_json_with_json(
      con = con,
      data = body,
      path = var_path(variableID),
      method = "PATCH"
    )

  }

#' Replace an influxdb variable
#'
#' @param con the database connection previously established with \code{connect}
#' @param variableID ID of the variable to update
#' @param  name of the variable
#' @param arguments value of the variable.  It should be the output of either
#' \code{query_variable_properties}, \code{constant_variable_properties}, or
#' \code{map_variable_properties}
#' @param createdAt date the parameter was created
#' @param description string description of the variable
#' @param labels labels Labels to create/attach to the variable. Should be the output of
#' the \code{variable_labels} function
#' @param selected default key to use if the variable store multiple key/value pairs
#' @param updatedAt date the parameter was last updated
#'
#' @return Influx V2 API json response
#'
#' @export
#'
replace_variable <-
  function (con,
            variableID,
            name,
            arguments,
            createdAt = NULL,
            description = NULL,
            labels = NULL,
            selected = NULL,
            updatedAt = NULL) {

    check_char(variableID)
    check_char(name)
    check_char_or_NULL(createdAt)
    check_char_or_NULL(description)
    check_char_or_NULL(updatedAt)
    stopifnot(is.list(arguments))
    stopifnot(is.null(labels) || is.list(labels))
    stopifnot(is.null(selected) || is.list(selected))

    if (name %in% c(
      "and",
      "import",
      "not",
      "return",
      "option",
      "test",
      "empty",
      "in",
      "or",
      "package",
      "builtin"
    ) ||
    !stringr::str_detect(name, "^[_a-zA-Z]")) {
      stop("Illegal variable name - ", name)
    }

    body <- drop_nulls(
      name = name,
      arguments = arguments,
      orgID = con$orgID,
      createdAt = createdAt,
      description = description,
      updatedAt = updatedAt,
      labels = labels,
      selected = selected
    )

    respond_to_json_with_json(
      con = con,
      path = var_path(variableID),
      data = body,
      method = "PUT"
    )

  }

#' List the influx labels associated with the influx variableID
#'
#' @param con the influx connection previously established with \code{connect}
#' @param variableID id of the variable whose labels are to be listed
#'
#' @return Influx V2 API json response
#' @export
#'
list_variable_labels <- function (con, variableID) {
  check_influxdb_con(con)
  check_char(variableID)
  respond_with_json(con, path = paste0(var_path(variableID), "/labels"))
}

#' Associate an influx labelID with an influx variableID
#'
#' @param con the connection previously established with \code{connect}
#' @param labelID id of the label to associate
#' @param variableID id of the label to associate
#'
#' @return Influx V2 API \code{httr::GET} response
#' @export
#'
add_label_to_variable <- function (con, labelID, variableID) {
  check_influxdb_con(con)
  check_char(labelID)
  check_char(variableID)

  body <- list(labelID = labelID)

  respond_to_json_with_json(
    con = con,
    data = body,
    path = paste0(var_path(variableID), "/labels"),
    method = "POST"
  )
}

#' Delete the association of an influx labelID with an influx variableID
#'
#' @param con the connection previously established with \code{connect}
#' @param labelID id of the label to associate
#' @param variableID id of the label to associate
#'
#' @return Influx V2 API json response
#' @export
#'
delete_label_from_variable <- function (con, labelID, variableID) {
  check_influxdb_con(con)
  check_char(labelID)
  check_char(variableID)

  respond(
    con = con,
    path = paste0(var_path(variableID), "/labels/", labelID),
    method = "DELETE"
  )
}
