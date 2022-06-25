# Generate a mocking function closure to use in testthat
mock_generator <- function (host = "myserver.mycompany.com", port = "8086") {
  mdb <- MockInfluxDB$new(host = host, port = port)
  function (req) {
    mdb$mock(req)
  }
}

# helper functions  ----------------------

new_id <- function () {
  format(stats::runif(1, 1000000000, 9999999999), nsmall = 0)
}

new_org <- function (id = NULL,
                     name = "",
                     description = "",
                     createdAt = Sys.time(),
                     updatedAt = Sys.time()) {
  if (is.null(id)) {
    id <- new_id()
  }
  list(
    id = id,
    name = name,
    description = description,
    createdAt = createdAt,
    updatedAt = updatedAt,
    # TODO links
    links = NULL
  )
}

new_auth <- function (id = NULL,
                      token = NULL,
                      org = NULL,
                      orgID = NULL,
                      user = NULL,
                      userID = NULL,
                      status = "active",
                      description = "",
                      permissions = NULL,
                      links = NULL,
                      createdAt = Sys.time(),
                      updatedAt = Sys.time()) {
  if (is.null(id)) {
    id <- new_id()
  }
  if (is.null(token)) {
    token <- paste0("token_", new_id())
  }
  list(
    id = id,
    token = token,
    status = status,
    description = description,
    orgID = orgID,
    org = org,
    userID = userID,
    user = user,
    # TODO permissions
    permissions = NULL,
    # TODO links
    links = NULL,
    createdAt = createdAt,
    updatedAt = updatedAt
  )
}

# response formatters -----------------------
respond_unauthorized <- function () {
  body <- charToRaw("{\"code\":\"unauthorized\",\"message\":\"unauthorized access\"}")
  resp <- httr2::response(
    status = 401,
    headers = list(
      `content-type` = "application/json; charset=utf-8",
      `x-influxdb-build` = "influxr",
      `x-influxdb-version` = "NA",
      `x-platform-error-code` = "unauthorized",
      `content-length` = length(body),
      `date` = Sys.time()
    ),
  )
  return(resp)
}

respond_empty <- function () {
  body <- charToRaw("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta name=\"description\" content=\"InfluxDB is a time series platform, purpose-built by InfluxData for storing metrics and events, provides real-time visibility into stacks, sensors, and systems.\"><title>InfluxDB</title><base href=\"/\"><link rel=\"shortcut icon\" href=\"/favicon.ico\"></head><body><div id=\"react-root\" data-basepath=\"\"></div><script src=\"/cc16dd8913.js\"></script></body></html>")
  resp <- httr2::response(
    status = 200,
    headers = list(
      `accept-ranges` = "bytes",
      `cache-control` = "public, max-age=3600",
      `content-type` = "text/html; charset=utf-8",
      `etag` = "5152183225",
      `last-modified` = Sys.time(),
      `x-influxdb-build` = "influxr",
      `x-influxdb-version` = "NA",
      `date` = Sys.time()
    ),
    body = body
  )
  return(resp)
}

respond_ping <- function () {
  resp <- httr2::response(
    status = 204,
    headers = list(
      `x-influxdb-build` = "influxr",
      `x-influxdb-version` = "NA",
      `date` = Sys.time()
    )
  )
  return(resp)
}

# mock class  -------------------------

MockInfluxDB <- setRefClass(
  "MockInfluxDB",
  fields = list(
    mdb_host = "character",
    mdb_port = "character",
    mdb_auths = "ANY",
    mdb_buckets = "ANY",
    mdb_labels = "ANY",
    mdb_orgs = "ANY",
    mdb_secrets = "ANY",
    mdb_users = "ANY",
    mdb_variables = "ANY"
  )
)

MockInfluxDB$methods(
  initialize = function (host = "myserver.mycompany.com", port = "8086") {
    mdb_host <<- host
    mdb_port <<- port

    default_org <- new_org(id = "default-org", name = "my-org")
    default_auth <- new_auth(
      id = "default-token",
      token = "my-token",
      org = default_org$name,
      orgID = default_org$id
    )

    mdb_auths <<- list(default_auth)
    mdb_buckets <<- list()
    mdb_labels <<- list()
    mdb_orgs <<- list(default_org)
    mdb_secrets <<- list()
    mdb_users <<- list()
    mdb_variables <<- list()
  }
)

MockInfluxDB$methods(
  authenticate = function (token) {
    if (is.null(token)) {
      return(FALSE)
    }
    strings <- stringr::str_split_fixed(token, " ", n = 2)
    if (strings[[1]] != "Token") {
      return (FALSE)
    }
    token <- strings[[2]]
    return(purrr::some(mdb_auths, ~ .x$token == token))
  }
)

MockInfluxDB$methods(
  mock = function (req) {
    url_parts <- httr2::url_parse(req$url)
    scheme <- url_parts$scheme
    hostname <- url_parts$hostname
    username <- url_parts$username
    password <- url_parts$password
    port <- url_parts$port
    path <- url_parts$path
    query <- url_parts$query
    fragment <- url_parts$fragment
    token <- req$headers$Authorization
    data <- req$body$data

    # Validate host/port URL
    if (is.null(hostname) ||
        is.null(port) ||
        hostname != mdb_host ||
        port != mdb_port) {
      stop(paste0("Could not resolve host: ", hostname), call. = FALSE)
    }

    # Validate authentication
    if (isFALSE(authenticate(token))) {
      return(respond_unauthorized())
    }

    # Get http mock
    http_response <- httr2::req_dry_run(req, quiet = TRUE)
    method <- http_response$method

    # Assume the server ignores everything else if the path is missing
    if (is.null(path)) {
      return(respond_empty())
    }

    if (path == "/ping") {
      return(respond_ping())
    }

    if (path == "/api/v2/orgs") {
      return(mock_orgs(query))
    }

    return(NULL)
  }
)

MockInfluxDB$methods(
  mock_orgs = function (query) {
    orgs <- mdb_orgs

    # Filter orgs based on query parameters
    # TODO additional filters like userID
    if (is.character(query$org)) {
      orgs <-
        orgs[unlist(lapply(orgs, function (org)
          org$name == query$org))]
    }
    if (is.character(query$orgID)) {
      orgs <-
        orgs[unlist(lapply(orgs, function (org)
          org$orgID == query$orgID))]
    }

    # If not matching orgs, return an error
    if (length(orgs) == 0) {
      body <- charToRaw("{\n\t\"code\": \"not found\",\n\t\"message\": \"organization not found\"\n}")
      resp <- httr2::response(
        status = 404,
        headers = list(
          `content-type` = "application/json; charset=utf-8",
          `x-influxdb-build` = "influxr",
          `x-influxdb-version` = "NA",
          `x-platform-error-code` = "not found",
          `content-length` = length(body),
          `date` = Sys.time()
        )
      )
      return(resp)
    }

    # return all matching orgs
    body <- charToRaw(toJSON(list(links = NULL, orgs = orgs)))
    resp <- httr2::response(
      status = 200,
      headers = list(
        `content-type` = "application/json; charset=utf-8",
        `content-length` = length(body),
        `date` = Sys.time()
      ),
      body = body
    )
    return(resp)
  }
)
