library(testthat)

test_that("batchApply", {
  andromeda <- andromeda()
  andromeda$cars <- cars

  doSomething <- function(batch, multiplier) {
    return(nrow(batch) * multiplier)
  }
  result <- batchApply(andromeda$cars, doSomething, multiplier = 2, batchSize = 10)
  result <- unlist(result)

  expect_true(sum(result) == nrow(cars) * 2)
  expect_true(length(result) == ceiling(nrow(cars)/10))
  close(andromeda)
})

test_that("batchApply safe mode", {
  andromeda <- andromeda()
  andromeda$cars <- cars

  doSomething <- function(batch, multiplier) {
    batch$speedSquared <- batch$speed^2
    if (is.null(andromeda$cars2)) {
      andromeda$cars2 <- batch
    } else {
      appendToTable(andromeda$cars2, batch)
    }
  }
  batchApply(andromeda$cars, doSomething, multiplier = 2, batchSize = 10, safe = TRUE)

  cars2 <- andromeda$cars2 %>% collect()
  cars1 <- cars
  cars1$speedSquared <- cars1$speed^2
  expect_equivalent(cars1, cars2)
  close(andromeda)
})

test_that("batchApply progress bar", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  
  doSomething <- function(batch, multiplier) {
    return(nrow(batch) * multiplier)
  }
  result <- batchApply(andromeda$cars, doSomething, multiplier = 2, batchSize = 10, showProgressBar = TRUE)
  result <- unlist(result)
  
  expect_true(sum(result) == nrow(cars) * 2)
  expect_true(length(result) == ceiling(nrow(cars)/10))
  close(andromeda)
})

test_that("groupApply", {
  andromeda <- andromeda()
  andromeda$cars <- cars

  doSomething <- function(batch, multiplier) {
    return(nrow(batch) * multiplier)
  }
  result <- groupApply(andromeda$cars, "speed", doSomething, multiplier = 2, batchSize = 10)
  result <- unlist(result)

  expect_true(sum(result) == nrow(cars) * 2)
  expect_true(length(result) == length(unique(cars$speed)))
  close(andromeda)
})

test_that("groupApply", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  
  doSomething <- function(batch, multiplier) {
    return(nrow(batch) * multiplier)
  }
  result <- groupApply(andromeda$cars, "speed", doSomething, multiplier = 2, batchSize = 10, showProgressBar = TRUE)
  result <- unlist(result)
  
  expect_true(sum(result) == nrow(cars) * 2)
  expect_true(length(result) == length(unique(cars$speed)))
  close(andromeda)
})

test_that("batchTest", {
  andromeda <- andromeda(cars = cars)
  
  isSpeedNotSorted <- function(batch) {
    return(is.unsorted(batch %>% select("speed") %>% collect()))
  }
  
  isSpeedSorted <- function(batch) {
    return(!is.unsorted(batch %>% select("speed") %>% collect()))
  }
  
  expect_true(batchTest(andromeda$cars, isSpeedNotSorted, batchSize = 5) == FALSE)
  expect_true(batchTest(andromeda$cars, isSpeedSorted, batchSize = 100) == TRUE)
})
