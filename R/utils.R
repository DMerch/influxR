#' Build an Influxdb 2.X API endpoint path
#'
#' @param path Path to append to the API endpoint
#' @param ... Additional elements to append to the path separated by /
#'
#' @return All inputs concatenated into a single / separated string
#'
api_path <- function (path, ...) {
  paste0(c("api/v2", path, ...), collapse = "/")
}


check_influxdb_con <- function (arg) {
  arg_symbol <- rlang::enexpr(arg)
  if (!(influxdb_class() %in% class(arg))) {
    call <- sys.call(-1)
    if (is.null(call)) {
      call <- sys.call()
    }
    stop(paste0(
      "Argument '",
      arg_symbol,
      "' of function ",
      as.list(call)[[1]],
      "() must be of type 'influxdb' instead of type '",
      class(arg),
      "'\n"
    ), call. = FALSE)
  }
}

check_char <- function (arg) {
  arg_symbol <- rlang::enexpr(arg)
  if (isFALSE(is.character(arg))) {
    call <- sys.call(-1)
    if (is.null(call)) {
      call <- sys.call()
    }
    stop(paste0(
      "Argument '",
      arg_symbol,
      "' of function ",
      as.list(call)[[1]],
      "() must be of type 'character' instead of type '",
      class(arg),
      "'\n"
    ), call. = FALSE)
  }
}

check_char_or_NULL <- function (arg) {
  arg_symbol <- rlang::enexpr(arg)
  if (isFALSE(is.null(arg) || is.character(arg))) {
    call <- sys.call(-1)
    if (is.null(call)) {
      call <- sys.call()
    }
    stop(paste0(
      "Argument '",
      arg_symbol,
      "' of function ",
      as.list(call)[[1]],
      "() must be of type 'character' instead of type '",
      class(arg),
      "'\n"
    ), call. = FALSE)
  }
}

check_integer_or_NULL <- function (arg) {
  arg_symbol <- rlang::enexpr(arg)
  if (isFALSE(is.null(arg) || is.integer(arg))) {
    call <- sys.call(-1)
    if (is.null(call)) {
      call <- sys.call()
    }
    stop(paste0(
      "Argument '",
      arg_symbol,
      "' of function ",
      as.list(call)[[1]],
      "() must be of type 'integer' instead of type '",
      class(arg),
      "'\n"
    ), call. = FALSE)
  }
}

check_mutually_exclusive <- function (arg1, arg2) {
  arg1_symbol <- rlang::enexpr(arg1)
  arg2_symbol <- rlang::enexpr(arg2)
  if (isFALSE(is.null(arg1) || is.null(arg2))) {
    call <- sys.call(-1)
    if (is.null(call)) {
      call <- sys.call()
    }
    stop(paste0(
      "Argments '",
      arg1_symbol,
      "' and '",
      arg2_symbol,
      "' of function ",
      as.list(call)[[1]],
      "() are mutually exlusive\n"
    ), call. = FALSE)
  }
}

drop_nulls <- function(...) {
  all_names <- list(...)
  all_names[lengths(all_names) != 0]
}
