library(testthat)

test_that("Date conversion", {
  andromeda <- andromeda()

  data <- data.frame(person_id = c(1, 2, 3),
                     startDate = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")),
                     someText = c("asdf", "asdf", "asdf"))
  
  andromeda$data <- data
  
  data2 <- andromeda$data %>% collect()
  
  expect_is(data2$startDate, "numeric")
  
  data2$startDate <- restoreDate(data2$startDate)
  
  expect_equal(data$startDate, data2$startDate)
  
  close(andromeda)
})

test_that("Time conversion", {
  andromeda <- andromeda()
  
  data <- data.frame(person_id = c(1, 2, 3),
                     startTime = as.POSIXct(c("2000-01-01 10:00", "2001-01-31 11:00", "2004-12-31")),
                     someText = c("asdf", "asdf", "asdf"))
  
  andromeda$data <- data
  
  data2 <- andromeda$data %>% collect()
  
  expect_is(data2$startTime, "numeric")
  
  data2$startTime <- restorePosixct(data2$startTime)
  
  expect_equal(data$startTime, data2$startTime)
  
  close(andromeda)
})
