library(testthat)

test_that("Index creation. listing, and removal using column names", {
  andromeda <- andromeda(cars = cars)
  createIndex(andromeda$cars, "speed")
  indices <- listIndices(andromeda$cars)
  expect_true(grepl("speed", indices$sql))
  
  createIndex(andromeda$cars, c("speed", "dist"))

  removeIndex(andromeda$cars, "speed")
  indices <- listIndices(andromeda$cars)
  expect_true(nrow(indices) == 1)
  
  removeIndex(andromeda$cars, c("speed", "dist"))
  indices <- listIndices(andromeda$cars)
  expect_true(nrow(indices) == 0)
  
  expect_error(removeIndex(andromeda$cars, "speed"))
  expect_error(createIndex(andromeda$cars, "foobar"))
  
  close(andromeda)
})

test_that("Index creation. listing, and removal using index name", {
  andromeda <- andromeda(cars = cars)
  createIndex(andromeda$cars, "speed", indexName = "myname")
  indices <- listIndices(andromeda$cars)
  expect_true(indices$index_name == "myname")
  removeIndex(andromeda$cars, indexName = "myname")
  indices <- listIndices(andromeda$cars)
  expect_true(nrow(indices) == 0)
  expect_error(removeIndex(andromeda$cars, indexName = "myname"))

  close(andromeda)
})

test_that("index functions check argument types", {
  expect_error(createIndex(iris, "Species"))
  expect_error(listIndices(iris))
  expect_error(removeIndex(iris, "Species"))
})

