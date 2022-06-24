buckets_path <- function(...) {
  api_path("buckets", ...)
}

#' Get the list of influx buckets
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param name Only return the bucket with the specified name
#' @param after The last resource ID from which to seek from.  Use this instead of offset.
#' @param id  Only return buckets with a specific ID.
#' @param limit Integer limit of the number of buckets to return (default 20)
#' @param offset Integer >- 0
#' @param org The name of the organization
#' @param orgID The organization ID
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_buckets <-
  function (con,
            name = NULL,
            after = NULL,
            id = NULL,
            limit = NULL,
            offset = NULL,
            org = NULL,
            orgID = NULL) {
    check_influxdb_con(con)
    check_char_or_NULL(name)
    check_char_or_NULL(after)
    check_char_or_NULL(id)
    check_integer_or_NULL(limit)
    check_integer_or_NULL(offset)
    check_char_or_NULL(org)
    check_char_or_NULL(orgID)
    if (is.integer(limit)) {
      stopifnot(limit >= 1L || limit <= 100L)
    }
    if (is.integer(offset)) {
      stopifnot(offset >= 0)
    }
    query <- drop_nulls(
      after = after,
      id = id,
      limit = limit,
      name = name,
      offset = offset,
      org = org,
      orgID = orgID
    )
    respond_with_json(con, path = buckets_path(), query = query)
  }

#' Create a bucket on the influx server
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param name name of the bucket
#' @param orgID organization ID
#' @param retentionRules influx retention rules for the bucket
#' @param description description of the bucket
#' @param rp undefined string - TBD
#' @param schemaType "implicit" or "explicit"
#'
#' @return Influxdb V2 API json response
#' @export
#'
create_bucket <-
  function (con,
            name,
            orgID = NULL,
            retentionRules = retentionRules_(),
            description = NULL,
            rp = NULL,
            schemaType = NULL) {
    check_influxdb_con(con)
    check_char_or_NULL(description)
    check_char(name)
    check_char_or_NULL(orgID)
    if (is.null(orgID)) {
      orgID <- con$orgID
    }
    stopifnot(is.list(retentionRules))
    check_char_or_NULL(rp)
    check_char_or_NULL(schemaType)
    if (isFALSE(is.null(schemaType))) {
      stopifnot(schemaType %in% c("implicit", "explicit"))
    }
    body <- drop_nulls(
      description = description,
      name = name,
      orgID = orgID,
      retentionRules = retentionRules,
      rp = rp,
      schemaType = schemaType
    )
    respond_to_json_with_json(con = con,
                              data = body,
                              path = buckets_path())
  }

#' Delete a bucket from the influx database
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param bucketID influx bucketID of the bucket to delete
#'
#' @return Influxdb V2 API http response
#' @export
#'
delete_bucket <- function (con, bucketID) {
  check_influxdb_con(con)
  check_char(bucketID)
  respond(con = con,
          path = buckets_path(bucketID),
          method = "DELETE")
}


#' Retrieve a bucket by bucketID from the influx database
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param bucketID the ID of the bucket to retrieve
#'
#' @return Influxdb V2 API json response
#' @export
#'
retrieve_bucket <- function (con, bucketID) {
  check_influxdb_con(con)
  check_char(bucketID)
  respond_with_json(con, path = buckets_path(bucketID))
}


#' Update a bucket on the influx server by bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param bucketID the ID of the bucket to update
#' @param name new name of the bucket if set
#' @param description new description of the bucket if set
#' @param retentionRules new influx retention rules for the bucket if set
#'
#' @return Influxdb V2 API json response
#' @export
#'
update_bucket <- function (con,
                           bucketID,
                           name = NULL,
                           description = NULL,
                           retentionRules = NULL) {
  check_influxdb_con(con)
  check_char(bucketID)
  check_char_or_NULL(name)
  check_char_or_NULL(name)
  stopifnot(is.null(retentionRules) || is.list(retentionRules))
  body <- drop_nulls(description = description,
                     name = name,
                     retentionRules = retentionRules)
  respond_to_json_with_json(
    con = con,
    data = body,
    path = buckets_path(bucketID),
    method = "PATCH"
  )
}


#' Create a retention rule for a bucket
#'
#' @param type default "expire"
#' @param everySeconds  >= 0; 0 means infinite
#' @param shardGroupDurationSeconds Shard duration measured in seconds. Optional.
#'
#' @return a retention rule data structure
#' @export
#'
#' @examples
#' retentionRules()
#' retentionRules(everySeconds = 60L * 60L * 24L * 7L) # weekly
retentionRules <- function (type = "expire",
                            everySeconds = 0L,
                            shardGroupDurationSeconds = NULL) {
  check_char(type)
  stopifnot(is.integer(everySeconds))
  stopifnot(everySeconds >= 0)
  check_integer_or_NULL(shardGroupDurationSeconds)
  list(
    drop_nulls(
      type = type,
      everySeconds = everySeconds,
      shardGroupDurationSeconds = shardGroupDurationSeconds
    )
  )
}

retentionRules_ <- retentionRules

#' List the influx labels associated with the influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param bucketID id of the bucket whose labels are to be listed
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_bucket_labels <- function (con, bucketID) {
  check_influxdb_con(con)
  check_char(bucketID)
  respond_with_json(con, path = buckets_path(bucketID, "labels"))
}

#' Associate an influx labelID with an influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param labelID id of the label to associate
#' @param bucketID id of the label to associate
#'
#' @return Influxdb V2 API json response
#' @export
#'
add_label_to_bucket <- function (con, labelID, bucketID) {
  check_influxdb_con(con)
  check_char(labelID)
  check_char(bucketID)
  body <- list(labelID = labelID)
  respond_to_json_with_json(con = con,
                            data = body,
                            path = buckets_path(bucketID, "labels"))
}

#' Delete the association of an influx labelID with an influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param labelID id of the label to associate
#' @param bucketID id of the label to associate
#'
#' @return Influxdb V2 API json response
#' @export
#'
delete_label_from_bucket <- function (con, labelID, bucketID) {
  check_influxdb_con(con)
  check_char(labelID)
  check_char(bucketID)
  respond(
    con = con,
    path = buckets_path(bucketID, "labels", labelID),
    method = "DELETE"
  )
}

#' List the influx members associated with the influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param bucketID id of the bucket whose members are to be listed
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_bucket_members <- function (con, bucketID) {
  check_influxdb_con(con)
  check_char(bucketID)
  respond_with_json(con, path = buckets_path(bucketID, "members"))
}

#' Associate an influx userID with an influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param id id of the member to associate
#' @param name optional name of the member to associate
#' @param bucketID id of the bucket to associate
#'
#' @return Influxdb V2 API json response
#' @export
#'
add_member_to_bucket <- function (con, id, bucketID, name = NULL) {
  check_influxdb_con(con)
  check_char(id)
  check_char(bucketID)
  check_char_or_NULL(name)
  body <- drop_nulls(id = id, name = name)
  respond_to_json_with_json(
    con = con,
    data = body,
    path = buckets_path(bucketID, "members"),
  )
}

#' Delete the association of an influx memberID with an influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param userID id of the member to disassociate
#' @param bucketID id of the bucket to disassociate
#'
#' @return Influxdb V2 API http response
#' @export
#'
remove_member_from_bucket <- function (con, userID, bucketID) {
  check_influxdb_con(con)
  check_char(userID)
  check_char(bucketID)
  respond(
    con = con,
    path = buckets_path(bucketID, "members", userID),
    method = "DELETE"
  )
}

#' List the influx owners associated with the influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param bucketID id of the bucket whose owners are to be listed
#'
#' @return Influxdb V2 API json response
#' @export
#'
list_bucket_owners <- function (con, bucketID) {
  check_influxdb_con(con)
  check_char(bucketID)
  respond_with_json(con, path = buckets_path(bucketID, "owners"))
}

#' Associate an influx user with an influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param id id of the owner to associate
#' @param bucketID id of the bucket to associate
#' @param name optional name of the user being added
#'
#' @return Influxdb V2 API json response
#' @export
#'
add_owner_to_bucket <- function (con, id, bucketID, name = NULL) {
  check_influxdb_con(con)
  check_char(id)
  check_char(bucketID)
  check_char_or_NULL(name)
  body <- drop_nulls(id = id, name = name)
  respond_to_json_with_json(
    con = con,
    data = body,
    path = buckets_path(bucketID, "owners")
  )
}

#' Delete the association of an influx ownerID with an influx bucketID
#'
#' @param con influxdb connection previously established with \code{connect}
#' @param userID id of the owner to disassociate
#' @param bucketID id of the bucket to disassociate
#'
#' @return Influxdb V2 API http response
#' @export
#'
remove_owner_from_bucket <- function (con, userID, bucketID) {
  check_influxdb_con(con)
  check_char(userID)
  check_char(bucketID)
  respond(
    con = con,
    path = buckets_path(bucketID, "owners", userID),
    method = "DELETE"
  )
}
