library(testthat)
library(dplyr)

test_that("Object creation", {
  andromeda <- Andromeda()
  expect_true(is.Andomeda(andromeda))
  close(andromeda)
  expect_error(names(andromeda))
})

test_that("Tables from data frames", {
  andromeda <- Andromeda()
  andromeda$cars <- cars
  expect_true("cars" %in% names(andromeda))
  cars2 <- andromeda$cars %>% collect()
  expect_equivalent(cars2, cars)
  close(andromeda)
})

test_that("Tables from tables", {
  andromeda <- Andromeda()
  andromeda$cars <- cars
  
  andromeda$cars2 <- andromeda$cars
  cars2 <- andromeda$cars2 %>% collect()
  expect_equivalent(cars2, cars)
  
  andromeda$cars3 <- andromeda$cars %>% filter(speed > 10)
  cars3 <- andromeda$cars3 %>% collect()
  expect_equivalent(cars3, cars %>% filter(speed > 10))
  
  andromeda2 <- Andromeda()
  andromeda2$cars <- cars
  andromeda$cars4 <- andromeda2$cars
  cars4 <- andromeda$cars4 %>% collect()
  expect_equivalent(cars4, cars)
  
  close(andromeda)
  close(andromeda2)
})

test_that("Dropping tables", {
  andromeda <- Andromeda()
  
  andromeda$cars <- cars
  expect_true("cars" %in% names(andromeda))
  
  andromeda$cars <- NULL
  expect_false("cars" %in% names(andromeda))
  close(andromeda)

})