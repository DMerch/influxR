secrets_path <- function (orgID, ...) {
  api_path("orgs", orgID, "secrets", ...)
}

#' List the influxdb secrets associated with an influxdb orgID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param orgID id of the organization whose secrets are to be listed
#'
#' @return influxdb V2 API json response
#' @export
#'
list_secrets <- function (con, orgID) {
  check_influxdb_con(con)
  check_char(orgID)
  respond_with_json(con, path = secrets_path(orgID))
}

#' Associate an influx user with an influx orgID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param secrets list of key/value pairs
#' @param orgID id of the organization with which to associate the secret
#'
#' @return influxdb V2 API json response
#' @export
#'
update_secrets <- function (con, orgID, secrets) {
  check_influxdb_con(con)
  check_char(orgID)
  stopifnot(is.list(secrets))
  secrets <- as.list(secrets)
  body <- list(secrets = secrets)
  respond_to_json(
    con = con,
    data = body,
    path = secrets_path(orgID),
    method = "PATCH"
  )
}

#' Delete the association of an influxdb secretID with an influx orgID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param secretID id of the secrete to delete
#' @param orgID id of the organization to disassociate
#'
#' @return influxdb V2 API http response
#' @export
#'
delete_secret <- function (con, orgID, secretID) {
  check_influxdb_con(con)
  check_char(orgID)
  check_char(secretID)
  respond(
    con = con,
    path = secrets_path(orgID, secretID),
    method = "DELETE"
  )
}
