#' Append the path to the Influx V2 API endpoint
#'
#' @param path Path to append to the API endpoint
#'
#' @return Path provided preceded by "api/v2/"
#'
api_path <- function (path) {
  paste0("api/v2/", path)
}


#' Throw an error if the argument is not a valid influxdb connection
#'
#' @param con object to test
#'
#' @return ignored
#'
check_influxdb_con <- function (con) {
  stopifnot("Invalid influxdb connection" = influxdb_class() %in% class(con))
}
