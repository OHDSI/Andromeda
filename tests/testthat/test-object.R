library(testthat)
library(dplyr)

test_that("Object creation", {
  andromeda <- andromeda()
  expect_true(is.Andomeda(andromeda))
  close(andromeda)
  expect_error(names(andromeda))
})

test_that("Tables from data frames", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  expect_true("cars" %in% names(andromeda))
  cars2 <- andromeda$cars %>% collect()
  expect_equivalent(cars2, cars)
  close(andromeda)
})

test_that("Tables from tables", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  
  andromeda$cars2 <- andromeda$cars
  cars2 <- andromeda$cars2 %>% collect()
  expect_equivalent(cars2, cars)
  
  andromeda$cars3 <- andromeda$cars %>% filter(speed > 10)
  cars3 <- andromeda$cars3 %>% collect()
  expect_equivalent(cars3, cars %>% filter(speed > 10))
  
  andromeda2 <- andromeda()
  andromeda2$cars <- cars
  andromeda$cars4 <- andromeda2$cars
  cars4 <- andromeda$cars4 %>% collect()
  expect_equivalent(cars4, cars)
  
  close(andromeda)
  close(andromeda2)
})

test_that("Dropping tables", {
  andromeda <- andromeda()
  
  andromeda$cars <- cars
  expect_true("cars" %in% names(andromeda))
  
  andromeda$cars <- NULL
  expect_false("cars" %in% names(andromeda))
  close(andromeda)
})

test_that("Zero rows", {
  andromeda <- andromeda()
  
  andromeda$cars <- cars[cars$speed > 1000, ]
  expect_true("cars" %in% names(andromeda))
  
  count <- andromeda$cars %>% count() %>% collect()
  expect_equal(count$n, 0)  
  
  cars2 <- andromeda$cars %>% collect()
  expect_equal(nrow(cars2), 0)  
  
  andromeda$iris <- iris
  andromeda$iris2 <- andromeda$iris %>% filter(Sepal.Length > 100)
  
  count2 <- andromeda$iris2 %>% count() %>% collect()
  expect_equal(count2$n, 0)  
  
  close(andromeda)
})


test_that("Object cleanup", {
  andromeda <- andromeda()
  
  fileName <- andromeda@dbname
  expect_true(file.exists(fileName))
  
  close(andromeda)
  expect_false(file.exists(fileName))
  
  
  andromeda2 <- andromeda()
  
  fileName <- andromeda2@dbname
  expect_true(file.exists(fileName))
  
  rm(andromeda2)
  invisible(gc())
  expect_false(file.exists(fileName))
  
  
  })
