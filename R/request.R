#' Build a basic http request to an influxdb server
#'
#' Additional modifications can be made to the returned object before executing
#' the query with httr2::req_perform().
#'
#' @param con An influxdb connection previous established using connect()
#' @param path Optional string path to append to the base url
#' @param query Optional list of query name/value pairs to append to the url
#' @param method Set a custom HTTP method like HEAD, DELETE, PATCH, UPDATE, or
#'   OPTIONS. The default method is GET for requests without a body, and POST
#'   for requests with a body.
#'
#' @return An httr2 request object
#'
request <-
  function (con,
            path = NULL,
            query = NULL,
            method = NULL) {

    req <- con$base_request
    if (is.character(path)) {
      req <- httr2::req_url_path(req, path)
    }
    req <- httr2::req_url_query(req, !!!query)
    if (is.character(method)) {
      req <- httr2::req_method(req, method = method)
    }
    return(req)
  }

#' Build a basic http request to an influxdb server including a json body
#'
#' @param con An influxdb connection previous established using connect()
#' @param data A list of variable/value parameters converted to json variables
#' @param path Optional string path to append to the base url
#' @param query Optional list of query name/value pairs to append to the url
#' @param method Set a custom HTTP method like HEAD, DELETE, PATCH, UPDATE, or
#'   OPTIONS. The default method is GET for requests without a body, and POST
#'   for requests with a body.
#'
#' @return An httr2 request object
request_with_json <-
  function (con,
            data,
            path = NULL,
            query = NULL,
            method = NULL) {
    req <-
      request(con,
              path = path,
              query = query,
              method = method)
    json_req <- httr2::req_body_json(req, data)
    return(json_req)
  }

#' Retrieve an influxdb server http response to a basic query
#'
#' @param con An influxdb connection previous established using connect()
#' @param path Optional string path to append to the base url
#' @param query Optional list of query name/value pairs to append to the url
#' @param method Set a custom HTTP method like HEAD, DELETE, PATCH, UPDATE, or
#'   OPTIONS. The default method is GET for requests without a body, and POST
#'   for requests with a body.
#'
#' @return The httr2 formatted response from the server
#'
respond <-
  function (con,
            path = NULL,
            query = NULL,
            method = NULL) {
    req <-
      request(con,
              path = path,
              query = query,
              method = method)
    if (getOption("influxr.debug", FALSE) == TRUE) {
      print(httr2::req_dry_run(req))
    }
    resp <- httr2::req_perform(req)
    return(resp)
  }

#' Retrieve an influxdb server json response to a basic query
#'
#' @param con An influxdb connection previous established using connect()
#' @param path Optional string path to append to the base url
#' @param query Optional list of query name/value pairs to append to the url
#' @param method Set a custom HTTP method like HEAD, DELETE, PATCH, UPDATE, or
#'   OPTIONS. The default method is GET for requests without a body, and POST
#'   for requests with a body.
#'
#' @return The httr2 formatted response from the server
#'
respond_with_json <-
  function (con,
            path = NULL,
            query = NULL,
            method = NULL) {
    resp <-
      respond(
        con,
        path = path,
        query = query,
        method = method
      )
    json <- httr2::resp_body_json(resp)
    return(json)
  }

#' Build a http request asking for a json response
#'
#' @param con An influxdb connection previous established using connect()
#' @param path Optional string path to append to the base url
#' @param query Optional list of query name/value pairs to append to the url
#' @param query Query parameters to append to the url
#' @param method Set a custom HTTP method like HEAD, DELETE, PATCH, UPDATE, or
#'   OPTIONS. The default method is GET for requests without a body, and POST
#'   for requests with a body.
#'
#' @return An httr2 request object
respond_to_json <-
  function (con,
            data,
            path = NULL,
            query = NULL,
            method = NULL) {
    req <-
      request_with_json(
        con,
        data = data,
        path = path,
        query = query,
        method = method
      )
    if (getOption("influxr.debug", FALSE) == TRUE) {
      print(httr2::req_dry_run(req))
    }
    resp <- httr2::req_perform(req)
    return(resp)
  }

#' Build a influxdb httr2 server json request asking for a json response
#'
#' @param con An influxdb connection previous established using connect()
#' @param data A list of variable/value parameters converted to json variables
#' @param path Optional string path to append to the base url
#' @param query Optional list of query name/value pairs to append to the url
#' @param method Set a custom HTTP method like HEAD, DELETE, PATCH, UPDATE, or
#'   OPTIONS. The default method is GET for requests without a body, and POST
#'   for requests with a body.
#'
#' @return An httr2 request object
respond_to_json_with_json <-
  function (con,
            data,
            path = NULL,
            query = NULL,
            method = NULL) {
    resp <-
      respond_to_json(
        con,
        data = data,
        path = path,
        query = query,
        method = method
      )
    json <- httr2::resp_body_json(resp)
  }
