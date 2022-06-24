#' Create a connection to an influxdb database
#'
#' The \code{connect()} function validates that the url, org, and token are
#' valid and returns an s3 object containing that information.  The returned
#' connection object is the first argument of all other API functions and tells
#' those functions with which server to communicate. Users are not required to
#' close connections and an arbitrary number of connections can be established.
#'
#' @param url  The influxdb server URL
#' @param org  The influxdb organization name to use when accessing the server
#' @param token The influxdb user authorization token to use when accessing the
#'   server
#'
#' @return an influxdb s3 object that maintains the connection information
#'
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect(url = "https://myserver.mycompany.com:8086",
#'                org = "my-org",
#'                token = "my-token")
#' }
#'
connect <- function (url = getOption("influxr.url", NULL),
                     org = getOption("influxr.org", NULL),
                     token = getOption("influxr.token", NULL)) {

  check_char(url)
  check_char(org)
  check_char(token)

  con <- new_connection(url = url, org = org, token = token)

  # Try a simple ping to validate the url
  ping_resp <-
    con$base_request %>%
    httr2::req_url_path_append("ping") %>%
    httr2::req_perform()

  # Make sure the org and the authorization token are valid
  org_resp <-
    con$base_request %>%
    httr2::req_url_path_append("api/v2/orgs") %>%
    httr2::req_url_query(org = org) %>%
    httr2::req_error(
      is_error = function(resp)
        FALSE
    ) %>%
    httr2::req_perform()

  if (httr2::resp_status(org_resp) == 200) {
    json <- httr2::resp_body_json(org_resp)
    con$orgID <- json$orgs[[1]]$id
    return(con)
  }

  if (httr2::resp_status(org_resp) == 404) {
    stop(paste0("Invalid organization provided to connect()- '", org, "'\n"),
         call. = FALSE)
  }

  if (httr2::resp_status(org_resp) == 401) {
    stop("Unauthorized token provided to connect()", call. = FALSE)
  }

  stop(
    paste0(
      "Unknown error during connect() - HTTP ",
      httr2::resp_status(org_resp),
      " ",
      httr2::resp_status_desc(org_resp),
      "\n"
    ),
    call. = FALSE
  )

  return(con)
}

new_connection <- function (url, org, orgID, token) {
  base_request <-
    httr2::request(url) %>%
    httr2::req_headers(Authorization = paste("Token", token))

  structure(list(
    base_request = base_request,
    url = url,
    org = org,
    orgID = NULL
  ),
  class = influxdb_class())
}

#' Formatter for printing the influxdb class
#'
#' @param x The influxdb object passed to print
#' @param ... ignored
#'
#' @return the object as invisible
#'
#' @export
#'
#' @method print influxdb
#'
#' @examples
#' \dontrun{
#' con <- connect(url = "https://myserver.mycompany.com:8086",
#'                org = "my-org",
#'                token = "my-token")
#' print(con)
#' }
print.influxdb <- function (x, ...) {
  cat("<influxdb ", x$url, " org=", x$org, ">\n", sep = "")
  invisible(x)
}

#' Return the class name for an influxdb connection
#'
#' @return string class name
#' @export
#'
#' @examples
#' influxdb_class()
influxdb_class <- function() {
  "influxdb"
}
