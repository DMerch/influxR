#' Retrieve the health status of an instance
#'
#' @param con influxdb connection previously established with \code{connect}
#'
#' @return Influxdb API V2.0 json response
#' @export
#'
health <- function (con) {
  check_influxdb_con(con)
  respond_with_json(con, path = "health")
}

#' Get the readiness of an instance at startup
#'
#' @param con influxdb connection previously established with \code{connect}
#'
#' @return Influxdb API V2.0 json response
#' @export
#'
ready <- function (con) {
  check_influxdb_con(con)
  respond_with_json(con, path = "ready")
}
