test_that("connect() works", {
  expect_s3_class(httr2::with_mock(
    mock_generator(),
    connect(url = "https://myserver.mycompany.com:8086",
            org = "my-org",
            token = "my-token")
  ), "influxdb")

  expect_error(httr2::with_mock(
    mock_generator(),
    connect(url = "https://myserver.mycompany.com:8086",
            org = "bad-org",
            token = "my-token")
  ),
  "Invalid organization")

  expect_error(httr2::with_mock(
    mock_generator(),
    connect(url = "https://myserver.mycompany.com:8086",
            org = "my-org",
            token = "bad-token")
  ), "Unauthorized")

  expect_error(httr2::with_mock(
    mock_generator(),
    connect(url = "https://bad.server.com:8086", org = "my-org", token = "my-token")
  ),
  "Could not resolve host")
})
