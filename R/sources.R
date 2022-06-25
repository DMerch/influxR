sources_path <- function(...) {
  api_path("sources", ...)
}

#' List sources for an influxdb
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param org string optional organization name
#'
#' @return influxdb V2 API json response
#' @export
#'
list_sources <- function (con, org = NULL) {
  check_influxdb_con(con)
  check_char_or_NULL(org)
  query <- drop_nulls(org = org)
  respond_with_json(con, path = sources_path(), query = query)
}

#' Create an influxdb source
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param default boolean
#' @param defaultRP string
#' @param id string
#' @param insecureSkipVerify boolean
#' @param links list with named strings "buckets", "health", "query", "self"
#' @param metaUrl string <uri>
#' @param name string
#' @param orgID string
#' @param password string
#' @param sharedSecret string
#' @param telegraf string
#' @param token string
#' @param type string
#' @param url string <uri>
#' @param username string
#'
#' @return influxdb V2 API json response
#' @export
#'
create_source <- function (con,
                           default = NULL,
                           defaultRP = NULL,
                           id = NULL,
                           insecureSkipVerify = NULL,
                           links = NULL,
                           metaUrl = NULL,
                           name = NULL,
                           orgID = NULL,
                           password = NULL,
                           sharedSecret = NULL,
                           telegraf = NULL,
                           token = NULL,
                           type = NULL,
                           url = NULL,
                           username = NULL) {
  check_influxdb_con(con)
  stopifnot(is.null(default) || is.logical(default))
  check_char_or_NULL(defaultRP)
  stopifnot(is.null(insecureSkipVerify) ||
              is.logical(insecureSkipVerify))
  stopifnot(is.null(links) || is.list(links))
  check_char_or_NULL(metaUrl)
  check_char_or_NULL(name)
  check_char_or_NULL(orgID)
  check_char_or_NULL(password)
  check_char_or_NULL(sharedSecret)
  check_char_or_NULL(telegraf)
  check_char_or_NULL(token)
  check_char_or_NULL(type)
  stopifnot(type %in% c("v1", "v2", "self"))
  check_char_or_NULL(url)
  check_char_or_NULL(username)
  body <- drop_nulls(
    default = default,
    defaultRP = defaultRP,
    id = id,
    insecureSkipVerify = insecureSkipVerify,
    links = links,
    metaUrl = metaUrl,
    name = name,
    orgID = orgID,
    password = password,
    sharedSecret = sharedSecret,
    telegraf = telegraf,
    token = token,
    type = type,
    url = url,
    username = username
  )
  respond_to_json_with_json(con, data = body, path = sources_path())
}

#' Delete an influxdb source
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param sourceID string source identifier
#'
#' @return influxdb V2 API http response
#' @export
#'
delete_source <- function (con, sourceID) {
  check_influxdb_con(con)
  check_char(sourceID)
  respond(con, path = sources_path(sourceID), method = "DELETE")
}

#' Retrieve an influxdb source
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param sourceID string source identifier
#'
#' @return influxdb V2 API http response
#' @export
#'
retrieve_source <- function (con, sourceID) {
  check_influxdb_con(con)
  check_char(sourceID)
  respond_with_json(con, path = sources_path(sourceID))
}

#' Update an influxdb source
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param sourceID string source identifier
#' @param default boolean
#' @param defaultRP string
#' @param id string
#' @param insecureSkipVerify boolean
#' @param links list with named strings "buckets", "health", "query", "self"
#' @param metaUrl string <uri>
#' @param name string
#' @param orgID string
#' @param password string
#' @param sharedSecret string
#' @param telegraf string
#' @param token string
#' @param type string
#' @param url string <uri>
#' @param username string
#'
#' @return influxdb V2 API json response
#' @export
#'
update_source <- function (con,
                           sourceID,
                           default = NULL,
                           defaultRP = NULL,
                           id = NULL,
                           insecureSkipVerify = NULL,
                           links = NULL,
                           metaUrl = NULL,
                           name = NULL,
                           orgID = NULL,
                           password = NULL,
                           sharedSecret = NULL,
                           telegraf = NULL,
                           token = NULL,
                           type = NULL,
                           url = NULL,
                           username = NULL) {
  check_influxdb_con(con)
  check_char(sourceID)
  stopifnot(is.null(default) || is.logical(default))
  check_char_or_NULL(defaultRP)
  stopifnot(is.null(insecureSkipVerify) ||
              is.logical(insecureSkipVerify))
  stopifnot(is.null(links) || is.list(links))
  check_char_or_NULL(metaUrl)
  check_char_or_NULL(name)
  check_char_or_NULL(orgID)
  check_char_or_NULL(password)
  check_char_or_NULL(sharedSecret)
  check_char_or_NULL(telegraf)
  check_char_or_NULL(token)
  check_char_or_NULL(type)
  stopifnot(type %in% c("v1", "v2", "self"))
  check_char_or_NULL(url)
  check_char_or_NULL(username)
  body <- drop_nulls(
    default = default,
    defaultRP = defaultRP,
    id = id,
    insecureSkipVerify = insecureSkipVerify,
    links = links,
    metaUrl = metaUrl,
    name = name,
    orgID = orgID,
    password = password,
    sharedSecret = sharedSecret,
    telegraf = telegraf,
    token = token,
    type = type,
    url = url,
    username = username
  )
  respond_to_json_with_json(con,
                            data = body,
                            path = sources_path(sourceID),
                            method = "PATCH")
}

#' Get buckets from a source
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param sourceID the source ID.
#' @param org The name of the organization
#'
#' @return Influx V2 API json response
#' @export
#'
get_source_buckets <- function (con, sourceID, org = NULL) {
  check_influxdb_con(con)
  check_char(sourceID)
  check_char_or_NULL(org)
  query <- drop_nulls(org = org)
  respond_with_json(con,
                    path = sources_path(sourceID, "buckets"),
                    query = query)
}

#' Obtain the health information from an influxdb source
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param sourceID string source identifier
#'
#' @return influxdb V2 API json response
#' @export
#'
get_source_health <- function (con, sourceID) {
  check_influxdb_con(con)
  check_char(sourceID)
  respond_with_json(con, path = sources_path(sourceID, "health"))
}
