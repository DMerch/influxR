test_that("request() works", {
  con <- new_connection(url = "https://myserver.mycompany.com:8086",
                        org = "my-org",
                        token = "my-token")

  req <- request(con)
  expect_s3_class(req, "httr2_request")
  expect_identical(req$url, "https://myserver.mycompany.com:8086")
  expect_null(req$method)
  expect_null(req$body)
  expect_identical(req$headers, list(Authorization = "Token my-token"))

  req <- request(con, path = "endpoint")
  expect_identical(req$url, "https://myserver.mycompany.com:8086/endpoint")
  expect_null(req$method)
  expect_null(req$body)

  req <- request(con, query = list(a = 1, b = "five"))
  expect_identical(req$url, "https://myserver.mycompany.com:8086?a=1&b=five")
  expect_null(req$method)
  expect_null(req$body)

  req <- request(con, path = "endpoint", query = list(a = 1, b = "five"))
  expect_identical(req$url, "https://myserver.mycompany.com:8086/endpoint?a=1&b=five")
  expect_null(req$method)
  expect_null(req$body)

  req <- request(con, method = "POST")
  expect_identical(req$url, "https://myserver.mycompany.com:8086")
  expect_identical(req$method, "POST")
  expect_null(req$body)

})

test_that("request_with_json() works", {
  con <- new_connection(url = "https://myserver.mycompany.com:8086",
                        org = "my-org",
                        token = "my-token")

  req <- request_with_json(con, data = list(x = 1))
  expect_identical(req$body$data$x, 1)

  req <- request_with_json(con, data = list(x = "two"))
  expect_identical(req$body$data$x, "two")

})
