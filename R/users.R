users_path <- function (...) {
  api_path("users", ...)
}

#' Get the list of users available at the influxdb connection
#'
#' @param con the influx connection previously established with \code{connect}
#' @param name Only return the user with the specific name.
#' @param after The last resource ID from which to seek from.  Use this instead of offset.
#' @param id  Only return users with a specific ID.
#' @param limit Integer limit of the number of users to return (default 20)
#' @param offset Integer >- 0
#'
#' @return Influx V2 API \code{httr::GET} response
#' @export
#'
list_users <-
  function (con,
            name = NULL,
            after = NULL,
            id = NULL,
            limit = NULL,
            offset = NULL) {
    check_influxdb_con(con)
    check_char_or_NULL(name)
    check_char_or_NULL(after)
    check_integer_or_NULL(limit)
    check_integer_or_NULL(offset)
    if (is.integer(limit)) {
      stopifnot(limit >= 1 && limit <= 100)
    }
    if (is.integer(offset)) {
      stopifnot(offset >= 0)
    }
    query <- drop_nulls(
      after = after,
      id = id,
      limit = limit,
      name = name,
      offset = offset
    )
    respond_with_json(con, path = users_path(), query = query)
  }

#' Create a new influxdb user
#'
#' @param con the influx connection previously established with \code{connect}
#' @param name name of the user
#' @param oauthID string
#' @param status either "active" or "inactive"
#'
#' @return Influx V2 API \code{httr::POST} response
#' @export
#'
create_user <-
  function (con,
            name,
            oauthID = NULL,
            status = c("active", "inactive")) {
    check_influxdb_con(con)
    check_char(name)
    check_char_or_NULL(oauthID)
    status <- match.arg(status)
    body <- drop_nulls(name = name,
                       oauthID = oauthID,
                       status = status)
    respond_to_json_with_json(con = con,
                              data = body,
                              path = users_path())
  }


#' Delete a user from the influxdb database
#'
#' @param con the influx connection previously established with \code{connect}
#' @param userID influx userID of the user to delete
#'
#' @return Influx V2 API \code{httr::DELETE} response
#' @export
#'
delete_user <- function (con, userID) {
  check_influxdb_con(con)
  check_char(userID)
  respond(con, path = users_path(userID), method = "DELETE")
}

#' Update an influxdb user
#'
#' @param con the influx connection previously established with \code{connect}
#' @param userID string ID of the user to update
#' @param name string name of the user
#' @param oauthID string
#' @param status string either "active" or "inactive"
#'
#' @return Influx V2 API \code{httr::POST} response
#' @export
#'
update_user <-
  function (con,
            userID,
            name,
            oauthID = NULL,
            status = c("active", "inactive")) {
    check_influxdb_con(con)
    check_char(name)
    check_char_or_NULL(oauthID)
    status <- match.arg(status)
    body <- drop_nulls(name = name,
                       oauthID = oauthID,
                       status = status)
    respond_to_json_with_json(con = con,
                              data = body,
                              path = users_path(userID),
                              method = "PATCH")
  }

#' Retrieve a user by userID from the influxdb database
#'
#' @param con the influx connection previously established with \code{connect}
#' @param userID the ID of the user to retrieve
#'
#' @return Influx V2 API \code{httr::GET} response
#' @export
#'
retrieve_user <- function (con, userID) {
  check_influxdb_con(con)
  check_char(userID)
  respond_with_json(con, path = users_path(userID))
}

#' Update the password for the current influxdb user
#'
#' @param con the influx connection previously established with \code{connect}
#' @param password new password string
#'
#' @return Influx V2 API \code{httr::PUT} response
#' @export
#'
update_password <- function (con, password) {
  check_influxdb_con(con)
  check_char(password)
  body <- list(password = password)
  respond_to_json(
    con,
    data = body,
    path = api_path("me/password"),
    method = "PUT"
  )
}

#' Update an influxdb user password
#'
#' @param con the influx connection previously established with \code{connect}
#' @param userID id of the user
#' @param password new password for the user
#'
#' @return Influx V2 API \code{httr::POST} response
#' @export
#'
update_user_password <-
  function (con,
            userID,
            password) {
    check_influxdb_con(con)
    check_char(userID)
    check_char(password)
    body <- list(password = password)
    respond_to_json(
      con,
      data = body,
      path = users_path(userID, "password")
    )
  }

#' Retrieve the feature flags for the currently authenticated influxdb user
#'
#' @param con the influx connection previously established with \code{connect}
#'
#' @return Influx V2 API \code{httr::GET} response
#' @export
#'
retrieve_feature_flags <- function (con) {
  check_influxdb_con(con)
  respond_with_json(con, path = api_path("flags"))
}

#' Retrieve the currently authenticated influxdb user
#'
#' @param con the influx connection previously established with \code{connect}
#'
#' @return Influx V2 API \code{httr::GET} response
#' @export
#'
retrieve_current_user <- function (con) {
  check_influxdb_con(con)
  respond_with_json(con, path = api_path("me"))
}

