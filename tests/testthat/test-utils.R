test_that("api_path() works", {
  expect_equal(api_path("endpoint"), "api/v2/endpoint")
})

test_that("check_influxdb_con() works", {

  con <- new_connection(url = "https://myserver.mycompany.com:8086",
                        org = "my-org",
                        token = "my-token")

  test_function <- function (con) {
    check_influxdb_con(con)
  }

  expect_error(test_function(NULL), "test_function")
  expect_error(test_function("bad"), "test_function")
  expect_silent(test_function(con))

})

test_that("check_char() works", {

  test_function <- function (arg) {
    check_char(arg)
  }

  expect_error(test_function(NULL), "test_function")
  expect_error(test_function(1), "test_function")
  expect_silent(test_function("character"))

})

test_that("check_char_or_NULL() works", {

  test_function <- function (arg) {
    check_char_or_NULL(arg)
  }

  expect_silent(test_function(NULL))
  expect_error(test_function(1), "test_function")
  expect_silent(test_function("character"))

})

test_that("check_mutually_exclusive() works", {

  test_function <- function (arg1, arg2) {
    check_mutually_exclusive(arg1, arg2)
  }

  expect_silent(test_function(1, NULL))
  expect_silent(test_function(NULL, 1))
  expect_silent(test_function(NULL, NULL))
  expect_silent(test_function("a", NULL))
  expect_silent(test_function(NULL, "a"))
  expect_error(test_function(1, 1), "test_function")
})

test_that("drop_nulls() works", {
  empty_list <- list()
  names(empty_list) <- vector("character")

  expect_identical(drop_nulls(a = 1, b = "hello", c = "world", d = 2),
                   list(a = 1, b = "hello", c = "world", d = 2))
  expect_identical(drop_nulls(a = NULL, b = "hello", c = "world", d = 2),
                   list(b = "hello", c = "world", d = 2))
  expect_identical(drop_nulls(a = 1, b = NULL, c = "world", d = 2),
                   list(a = 1, c = "world", d = 2))
  expect_identical(drop_nulls(a = 1, b = "hello", c = "world", d = NULL),
                   list(a = 1, b = "hello", c = "world"))
  expect_identical(drop_nulls(a = NULL, b = NULL, c = NULL, d = NULL),
                   empty_list)
})
