library(testthat)

withr::local_options(list("andromedaTempFolder" = "~/andromedaTempFolder"))

test_that("Dates are preserved", {
  andromeda <- andromeda()

  data <- data.frame(person_id = c(1, 2, 3),
                     startDate = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")),
                     someText = c("asdf", "asdf", "asdf"))
  
  andromeda$data <- data
  
  data2 <- andromeda$data %>% collect()
  
  expect_is(data2$startDate, "Date")
  
  expect_equal(data$startDate, data2$startDate)
  
  # save and reload dates
  filename <- tempfile()
  saveAndromeda(andromeda, filename)
  andromeda <- loadAndromeda(filename)
  expect_is(pull(andromeda$data, startDate), "Date")
  expect_equal(pull(andromeda$data, startDate), data$startDate)
  
  close(andromeda)
})

test_that("Times are preserved", {
  andromeda <- andromeda()
  
  data <- data.frame(person_id = c(1, 2, 3),
                     startTime = as.POSIXct(c("2000-01-01 10:00:00", "2001-01-31 11:00:00", "2004-12-31 21:00:00"), tz = "UTC"),
                     someText = c("asdf", "asdf", "asdf"))
  
  tibble::tibble(data)
  andromeda$data <- data
  
  data2 <- andromeda$data %>% collect()
  
  expect_is(data2$startTime, "POSIXct")
  
  expect_equal(data$startTime, data2$startTime)
  
  # save and reload datetimes
  filename <- tempfile()
  saveAndromeda(andromeda, filename)
  andromeda <- loadAndromeda(filename)
  expect_is(pull(andromeda$data, startTime), "POSIXct")
  expect_equal(pull(andromeda$data, startTime), data$startTime)
  
  close(andromeda)
})

test_that("Date manipulations work", {
  andr <- andromeda()
  
  df <- data.frame(person_id = c(1, 2, 3),
                   startDate = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")),
                   endDate   = as.Date(c("2000-01-02", "2001-02-01", "2005-01-01")),
                   someText  = c("asdf", "asdf", "asdf"))
  
  andr$df <- df
  
  # add days to a date
  andr$df %>% 
    mutate(newEndDate = startDate + lubridate::ddays(1)) %>% 
    mutate(match = (endDate == newEndDate)) %>% 
    pull(match) %>% 
    all() %>% 
    expect_true()

  # conversion of date to integer
  andr$df %>% 
    mutate(endDateInteger = as.integer(endDate)) %>% 
    collect() %>% 
    pull(endDateInteger) %>% 
    expect_is("integer")
  
  # difference of dates in days
  andr$df %>% 
    mutate(diff = as.integer(endDate) - as.integer(startDate)) %>% 
    collect() %>% 
    pull(diff) %>% 
    expect_equal(c(1, 1, 1))
})

