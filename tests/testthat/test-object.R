library(testthat)

test_that("Object creation", {
  andromeda <- andromeda()
  expect_true(isAndomeda(andromeda))
  close(andromeda)
  expect_null(names(andromeda))
  
  andromeda <- andromeda(cars = cars, iris = iris)
  expect_true("cars" %in% names(andromeda))
  expect_true("iris" %in% names(andromeda))
  close(andromeda)
  
  # All arguments must be named:
  expect_error(andromeda(cars, iris))
})

test_that("Tables from data frames", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  expect_true("cars" %in% names(andromeda))
  cars2 <- andromeda$cars %>% collect()
  expect_equivalent(cars2, cars)
  
  andromeda[["USArrests"]] <- USArrests
  USArrests2 <- andromeda[["USArrests"]] %>% collect()
  expect_equivalent(USArrests2, USArrests)
  
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

test_that("dim function", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  expect_equal(nrow(andromeda$cars), nrow(cars))
  expect_equal(ncol(andromeda$cars), ncol(cars))
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

test_that("Setting the andromeda temp folder", {
  folder <- tempfile()
  options(andromedaTempFolder = folder)
  andromeda <- andromeda()
  files <- list.files(folder)
  expect_equal(length(files), 1)
  close(andromeda)
  unlink(folder, recursive = TRUE)
})

test_that("Copying andromeda", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  
  andromeda2 <- copyAndromeda(andromeda)
  
  expect_true("cars" %in% names(andromeda2))
  expect_false(andromeda@dbname == andromeda2@dbname)
  close(andromeda)
  close(andromeda2)
})