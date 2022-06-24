mdb_state <- NULL

mdb_reset_influxdb <- function () {
  mdb_state <<- mdb_generate_initial_state()
}

mdb_id <- function () {
  format(stats::runif(1, 1000000000, 9999999999), nsmall = 0)
}

mdb_org <-
  function (id = NULL,
            name = "",
            description = "",
            createdAt = Sys.time(),
            updatedAt = Sys.time()) {
    if (is.null(id)) {
      id <- mdb_id()
    }
    list(
      id = id,
      name = name,
      description = description,
      createdAt = createdAt,
      updatedAt = updatedAt,
      links = NULL   # may not need
    )
  }

mdb_authorization <-
  function (id = NULL,
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
      id <- mdb_id()
    }
    if (is.null(token)) {
      token <- paste0("token_", mdb_id())
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
      permissions = NULL, # TODO
      links = NULL, # TODO
      createdAt = createdAt,
      updatedAt = updatedAt
    )
  }

mdb_generate_initial_state <- function () {
  my_org <- mdb_org(id = "my-org-1", name = "my-org")
  my_token <-
    mdb_authorization(
      id = "token_my_token",
      token = "my-token-1",
      org = my_org$name,
      orgID = my_org$id
    )

  authorizations <- list(my_token)
  orgs <- list(my_org)

  list(
    authorizations = authorizations,
    orgs = orgs
  )
}

mock_unauthorized <- function () {
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

mock_empty <- function () {
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

mock_ping <- function () {
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

mock_orgs <- function (query) {
  orgs <- mdb_state$orgs

  # Filter orgs based on query parameters
  # TODO additional filters like userID
  if (is.character(query$org)) {
    orgs <- orgs[unlist(lapply(orgs, function (org) org$name == query$org))]
  }
  if (is.character(query$orgID)) {
    orgs <- orgs[unlist(lapply(orgs, function (org) org$orgID == query$orgID))]
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
  body <- charToRaw(jsonlite::toJSON(list(links = NULL, orgs = orgs)))
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

mock_influxdb <- function (req) {
  if (is.null(mdb_state)) {
    mdb_reset_influxdb()
  }

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

  dry_run <- httr2::req_dry_run(req)
  method <- dry_run$method

  # "authentication" ----
  # Make sure the server and port are valid and that the token is valid
  if (is.null(hostname) || is.null(port) || hostname != "myserver.mycompany.com" || port != "8086") {
    stop("Could not resolve host: myserver.mycompany.com", call. = FALSE)
  }
  if (is.null(token) || token != "Token my-token") {
    resp <- mock_unauthorized()
    return(resp)
  }

  # "no path" ----
  # Assume the server ignores everything else if the path is missing
  if (is.null(path)) {
    resp <- mock_empty()
    return(rep)
  }

  # "ping" ----
  if (path == "/ping") {
    resp <- mock_ping()
    return(resp)
  }

  if (path == "/api/v2/orgs"){
    resp <- mock_orgs(query)
    return(resp)
  }

  return(NULL)
}
