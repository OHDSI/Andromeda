library(testthat)
library(dplyr)

test_that("batchApply", {
  andromeda <- Andromeda()
  andromeda$cars <- cars
  
  doSomething <- function(batch, multiplier) {
    return(nrow(batch) * multiplier)
  }
  result <- batchApply(andromeda$cars, doSomething, multiplier = 2, batchSize = 10)
  result <- unlist(result)
  
  expect_true(sum(result) == nrow(cars)*2)
  expect_true(length(result) == ceiling(nrow(cars) / 10))
  close(andromeda)
})