auth_api <- function(...) {
  api_path("authorizations", ...)
}

#' List authorizations available on an influxdb server
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param org Only show authorizations that match the organization named
#'   (mutually exclusive with \code{orgID})
#' @param orgID Only show authorizations that match the organization id
#'   (mutually exclusive with \code{org})
#' @param user Only show authorizations that belong to the user named (mutually
#'   exclusive with \code{userID})
#' @param userID Only show authorizations that belong to the userID (mutually
#'   exclusive with \code{user})
#'
#' @return Influx V2 API json response
#' @export
#'
#' @examples
#' \dontrun{
#' con <- connect(url = "https://myserver.mycompany.com:8086",
#'                org = "my-org",
#'                token = "my-token")
#' all_auth <- list_authorizations(con)
#' org_auth <- list_authorizations(con, org = "my-org")
#' user_auth <- list_authorizations(con, user = "username")
#' }
list_authorizations <-
  function (con,
            org = NULL,
            orgID = NULL,
            user = NULL,
            userID = NULL) {
    check_influxdb_con(con)
    check_char_or_NULL(org)
    check_char_or_NULL(orgID)
    check_char_or_NULL(user)
    check_char_or_NULL(userID)
    check_mutually_exclusive(org, orgID)
    check_mutually_exclusive(user, userID)

    query <- drop_nulls(
      org = org,
      orgID = orgID,
      user = user,
      userID = userID
    )

    respond_with_json(con, path = auth_api(), query = query)
  }

#' Create an influx authorization
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param description description of the authorization
#' @param orgID ID of org that authorization is scoped to
#' @param permissions vector of authorized permissions created with \code{permissions} or with
#' predefined permissions through \code{all_permissions} and \code{all_read_permissions}
#' @param status if inactive the token is inactive and requests using the token will be rejected
#' @param userID ID of user that authorization is scoped to
#'
#' @return Influx V2 API json response
#' @export
#'
create_authorization <-
  function (con,
            description = NULL,
            orgID = con$orgID,
            permissions = all_read_permissions(),
            status = c("active", "inactive"),
            userID = NULL) {
    check_influxdb_con(con)
    check_char_or_NULL(description)
    check_char(orgID)
    status <- match.arg(status)
    check_char_or_NULL(userID)

    body <- drop_nulls(
      orgID = orgID,
      status = status,
      description = description,
      userID = userID,
      permissions = permissions
    )

    respond_to_json_with_json(con, data = body, path = auth_api())
  }

#' Update an authorization from the influx database
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param authID influx authorizationID of the authorization to update
#'
#' @return TRUE if existed and deleted; FALSE otherwise
#' @export
#'
delete_authorization <- function (con, authID) {
  check_influxdb_con(con)
  check_char(authID)
  respond(con, path = auth_api(authID), method = "DELETE")
}


#' Retrieve a authorization by authorization ID from the influx database
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param authID the ID of the authorization to retrieve
#'
#' @return Influxdb V2 API json response
#' @export
#'
retrieve_authorization <- function (con, authID) {
  check_influxdb_con(con)
  check_char(authID)
  respond_with_json(con, path = auth_api(authID))
}


#' Update an influxdb authorization
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param authID the ID of the authorization to retrieve
#' @param description String description of the authorization
#' @param status String "active" or "inactive"
#'
#' @return Influx V2 API json response
#' @export
#'
update_authorization <-
  function (con,
            authID,
            description = NULL,
            status = NULL) {
    check_influxdb_con(con)
    check_char(authID)
    check_char_or_NULL(description)
    check_char_or_NULL(status)
    stopifnot(status %in% c("active", "inactive"))

    body <- drop_nulls(description = description,
                       status = status)

    respond_to_json_with_json(
      con = con,
      data = body,
      path = auth_api(authID),
      method = "PATCH"
    )
  }


#' Return a character vector of all available resource types in influx
#'
#' @return character vector of all available resource types in influx
#' @export
#'
#' @examples
#' influxdb_resource_types()
influxdb_resource_types <- function () {
  c(
    "buckets",
    "authorizations",
    "dashboards",
    "orgs",
    "sources",
    "tasks",
    "telegrafs",
    "users",
    "variables",
    "scrapers",
    "secrets",
    "labels",
    "views",
    "documents",
    "notificationRules",
    "notificationEndpoints",
    "checks",
    "dbrp" #,
    # "notebooks"
  )
}

#' Return a resource data structure in required when creating permissions
#'
#' @param type the type of influx resource the permission will be applied to.
#' \code{influx_resource_types()} returns a complete list of available resource
#' types
#' @param id If id set, the permission applies to a specific resource.  Otherwise it applies
#' to all resources of that type
#' @param name  Optional name of the resource if it has a name
#' @param org  Optional name of the organization with the orgID
#' @param orgID If orgID is set, the permission is for all resources owned by that org.
#' If not, it is a permission for all resources of that resource type
#'
#' @return data structure representing a single resource.  The resource must still be combined
#' with an action ("read" or "write") to create a permission.
#'
#' @export
#'
#' @examples
#' resource(type = "buckets")
#' resource(type = "authorizations", org = "myorg")
resource <- function (type = influxdb_resource_types(),
                      id = NULL,
                      name = NULL,
                      org = NULL,
                      orgID = NULL) {
  type = match.arg(type)
  check_char_or_NULL(id)
  check_char_or_NULL(name)
  check_char_or_NULL(org)
  check_char_or_NULL(orgID)
  check_mutually_exclusive(org, orgID)

  drop_nulls(
    id = id,
    name = name,
    org = org,
    orgID = orgID,
    type = type
  )

}

#' Create a set of read and write permissions using the list of resources provided
#'
#' @param read list of resources to create read permissions for
#' @param write list of resources to create write permissions for
#' @param read_write list of resources to create both read and write permissions for
#'
#' @return permission data structure used when creating an authorization
#'with \code{create_authorization}
#' @export
#'
#' @examples
#' permissions(read_write = resource(type = "buckets"))
#' permissions(read = list(resource(type = "buckets"),
#'                         resource(type = "authorizations", org = "myorg")))
permissions <-
  function (read = NULL,
            write = NULL,
            read_write = NULL) {
   c(
      purrr::map(read, ~ list(
        action = "read", resource = .x
      )),
      purrr::map(read_write, ~ list(
        action = "read", resource = .x
      )),
      purrr::map(write, ~ list(
        action = "write", resource = .x
      )),
      purrr::map(read_write, ~ list(
        action = "write", resource = .x
      ))
    )
  }

#' Create a set of read/write permissions for all resources
#'
#' @return A permissions data structure that enables read/write access to all resources
#' @export
#'
#' @examples
#' all_permissions()
all_permissions <- function () {
  permissions(read_write = purrr::map(influxdb_resource_types(), ~ resource(type = .x)))
}

#' Create a set of read permissions for all resources
#'
#' @return A permissions data structure that enables read access to all resources
#' @export
#'
#' @examples
#' all_read_permissions()
all_read_permissions <- function () {
  permissions(read = purrr::map(influxdb_resource_types(), ~ resource(type = .x)))
}
