library(testthat)

test_that("Dates are preserved", {
  andromeda <- andromeda()

  data <- data.frame(personId = c(1, 2, 3),
                     startDate = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")),
                     someText = c("asdf", "asdf", "asdf"))
  
  # From R to Andromeda and back
  andromeda$data <- data
  data2 <- andromeda$data %>% collect()
  expect_is(data2$startDate, "Date")
  expect_equal(data$startDate, data2$startDate)
  
  # From Andromeda to Andromeda
  andromeda$data <- andromeda$data %>% 
    select(personId, startDate) 
  data3 <- andromeda$data %>% collect()
  expect_is(data3$startDate, "Date")
  expect_equal(data$startDate, data3$startDate)
  
  # Save and reload dates
  filename <- tempfile()
  saveAndromeda(andromeda, filename)
  andromeda <- loadAndromeda(filename)
  expect_is(pull(andromeda$data, startDate), "Date")
  expect_equal(pull(andromeda$data, startDate), data$startDate)
  
  close(andromeda)
})

test_that("Times are preserved", {
  andromeda <- andromeda()
  
  data <- data.frame(personId = c(1, 2, 3),
                     startTime = as.POSIXct(c("2000-01-01 10:00:00", "2001-01-31 11:00:00", "2004-12-31 21:00:00"), tz = "UTC"),
                     someText = c("asdf", "asdf", "asdf"))
  
  # From R to Andromeda and back
  andromeda$data <- data
  data2 <- andromeda$data %>% collect()
  expect_is(data2$startTime, "POSIXct")
  expect_equal(data$startTime, data2$startTime)
  
  # From Andromeda to Andromeda
  andromeda$data <- andromeda$data %>% 
    select(personId, startTime) 
  data2 <- andromeda$data %>% collect()
  expect_is(data2$startTime, "POSIXct")
  expect_equal(data$startTime, data2$startTime)
  
  # Save and reload datetimes
  filename <- tempfile()
  saveAndromeda(andromeda, filename)
  andromeda <- loadAndromeda(filename)
  expect_is(pull(andromeda$data, startTime), "POSIXct")
  expect_equal(pull(andromeda$data, startTime), data$startTime)
  
  close(andromeda)
})

# for duckdb I don't think we need restoreDate and restorePosixct anymore
# test_that("restore Dates works", {
#   
#   df <- dplyr::tibble(todayDate = Sys.Date(),
#                       anotherDate = Sys.Date())
#   a <- andromeda(df = df)
# 
#   # creating a new table in the sqlite database will convert dates to numbers
#   a$df2 <- a$df %>% 
#     select(todayDate, anotherDate)
#   
#   df2 <- a$df2 %>% 
#     collect() %>% 
#     mutate_all(restoreDate)
#   
#   expect_equal(df2, df)  
# })
# 
# 
# 
# test_that("restorePosixct  works", {
#   
#   df <- dplyr::tibble(now = Sys.time())
#   a <- andromeda(df = df)
# 
#   # creating a new table in the sqlite database will convert datetimes to numbers
#   a$df2 <- a$df %>% 
#     select(now)
#   
#   df2 <- a$df2 %>% 
#     collect() %>% 
#     mutate_all(restorePosixct)
#   
#   expect_equal(df2, df)  
# })


