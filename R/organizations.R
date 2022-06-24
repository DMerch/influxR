orgs_api <- function(...) {
  api_path("orgs", ...)
}

#' Get the list of organizations available at the influxdb connection
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param descending boolean
#' @param limit integer - default = 20
#' @param offset integer >- 0
#' @param org Filter organizations specific to the org name
#' @param orgID Filter organizations specific to the orgID
#' @param userID Fitler organizations to a specific user ID
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_organizations <-
  function (con,
            descending = NULL,
            limit = NULL,
            offset = NULL,
            org = NULL,
            orgID = NULL,
            userID = NULL) {
    check_influxdb_con(con)
    check_char_or_NULL(descending)
    check_integer_or_NULL(limit)
    check_integer_or_NULL(offset)
    check_char_or_NULL(org)
    check_char_or_NULL(orgID)
    check_char_or_NULL(userID)
    if (is.integer(limit)) {
      stopifnot(limit >= 1L || limit <= 100L)
    }
    if (is.integer(offset)) {
      stopifnot(offset >= 0)
    }
    query <- drop_nulls(
      descending = descending,
      limit = limit,
      offset = offset,
      org = org,
      orgID = orgID,
      userID = userID
    )
    respond_with_json(con, path = orgs_api(), query = query)
  }

#' Create a organization on the influxdb server
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param name name of the organization
#' @param description description of the organization
#'
#' @return Influxdb V2 API json response
#' @export
#'
create_organization <-
  function (con,
            name,
            description = NULL) {
    check_influxdb_con(con)
    check_char(name)
    check_char_or_NULL(description)
    body <- drop_nulls(name = name, description = description)
    respond_to_json_with_json(con, data = body, path = orgs_api())
  }

#' Delete a organization from the influxdb database
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param orgID influx orgID of the organization to delete
#'
#' @return Influxdb V2 API response
#' @export
#'
delete_organization <- function (con, orgID) {
  check_influxdb_con(con)
  check_char(orgID)
  respond(con = con, path = orgs_api(orgID), method = "DELETE")
}

#' Retrieve a organization by orgID from the influx database
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param orgID the ID of the organization to retrieve
#'
#' @return Influxdb V2 API json response
#' @export
#'
retrieve_organization <- function (con, orgID) {
  check_influxdb_con(con)
  check_char(orgID)
  respond_with_json(con, path = orgs_api(orgID))
}

#' Update a organization on the influx server by orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param orgID the ID of the organization to update
#' @param name new name of the organization if set
#' @param description new description of the organization if set
#'
#' @return Influxdb V2 API json response
#' @export
#'
update_organization <- function (con,
                                 orgID,
                                 name = NULL,
                                 description = NULL) {
  check_influxdb_con(con)
  check_char(orgID)
  check_char_or_NULL(name)
  check_char_or_NULL(description)
  body <- drop_nulls(name = name,
                     description = description)
  respond_to_json_with_json(
    con = con,
    data = body,
    path = orgs_api(orgID),
    method = "PATCH"
  )
}

#' List the influx members associated with the influx orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param orgID id of the organization whose members are to be listed
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_organization_members <- function (con, orgID) {
  check_influxdb_con(con)
  check_char(orgID)
  respond_with_json(con, path = orgs_api(orgID, "members"))
}

#' Associate an influx userID with an influx orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param id id of the member to associate
#' @param name optional name of the member to associate
#' @param orgID id of the organization to associate
#'
#' @return Influxdb V2 API json response
#' @export
#'
add_member_to_organization <-
  function (con, id, orgID, name = NULL) {
    check_influxdb_con(con)
    check_char(id)
    check_char(orgID)
    check_char_or_NULL(name)
    body <- drop_nulls(id = id, name = name)
    respond_to_json_with_json(
      con = con,
      data = body,
      path = orgs_api(orgID, "members"),
      method = "POST"
    )
  }

#' Delete the association of an influx memberID with an influx orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param userID id of the member to disassociate
#' @param orgID id of the organization to disassociate
#'
#' @return Influxdb V2 API response
#' @export
#'
remove_member_from_organization <- function (con, userID, orgID) {
  check_influxdb_con(con)
  check_char(userID)
  check_char(orgID)
  respond(
    con = con,
    path = orgs_api(orgID, "members", userID),
    method = "DELETE"
  )
}

#' List the influx owners associated with the influx orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param orgID id of the organization whose owners are to be listed
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_organization_owners <- function (con, orgID) {
  check_influxdb_con(con)
  check_char(orgID)
  respond_with_json(con, path = orgs_api(orgID, "owners"))
}

#' Associate an influx user with an influx orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param id id of the owner to associate
#' @param orgID id of the organization to associate
#' @param name optional name of the user being added
#'
#' @return Influxdb V2 API json response
#' @export
#'
add_owner_to_organization <- function (con, id, orgID, name = NULL) {
  check_influxdb_con(con)
  check_char(id)
  check_char(orgID)
  check_char_or_NULL(name)
  body <- drop_nulls(id = id, name = name)
  respond_to_json_with_json(
    con = con,
    data = body,
    path = orgs_api(orgID, "owners"),
    method = "POST"
  )
}

#' Delete the association of an influx ownerID with an influx orgID
#'
#' @param con influxdb connection previously established with \code{connect()}
#' @param userID id of the owner to disassociate
#' @param orgID id of the organization to disassociate
#'
#' @return Influxdb V2 API response
#' @export
#'
remove_owner_from_organization <- function (con, userID, orgID) {
  check_influxdb_con(con)
  check_char(userID)
  check_char(orgID)
  respond(
    con = con,
    path = orgs_api(orgID, "owners", userID),
    method = "DELETE"
  )
}
