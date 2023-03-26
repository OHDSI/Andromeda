library(testthat)

test_that("Object creation", {
  andromeda <- andromeda()
  expect_true(isAndromeda(andromeda))
  expect_true(isValidAndromeda(andromeda))

  close(andromeda)
  expect_error(names(andromeda), "no longer valid")
  expect_true(isAndromeda(andromeda))
  expect_false(isValidAndromeda(andromeda))

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

test_that("Table from same table in same Andromeda", {
  andromeda <- andromeda()
  andromeda$cars <- cars
  andromeda$cars <- andromeda$cars %>% filter(speed > 10)
  
  cars2 <- andromeda$cars %>% collect()
  expect_equivalent(cars2, cars %>% filter(speed > 10))
  
  close(andromeda)
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

  andromeda2 <- andromeda(iris2 = andromeda$iris2)
  
  count3 <- andromeda2$iris2 %>% count() %>% collect()
  expect_equal(count3$n, 0)
  
  close(andromeda)
  close(andromeda2)
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

test_that("Warning when disk space low", {
  skip_if(.Platform$OS.type != "windows")
  
  availableSpace <- tryCatch(getAndromedaTempDiskSpace(), 
                             error = function(e) NA)
  skip_if(is.na(availableSpace))

  andromeda <- andromeda()
  options(warnDiskSpaceThreshold = Inf)
  expect_warning(andromeda$cars <- cars)
  close(andromeda)
  options(warnDiskSpaceThreshold = NULL)
})

test_that("The only cached class is Andromeda", {
  expect_true(setequal(getClasses(asNamespace("Andromeda"), inherits = F), "Andromeda"))
})

test_that("Get/set Andromeda table/column names works.", {
  andr <- andromeda()
  expect_length(names(andr), 0L)
  
  andr$cars <- cars
  andr[["iris"]] <- iris
  expect_equal(names(andr), c("cars", "iris"))
  
  names(andr) <- c("cars", "table2")
  expect_equal(names(andr), c("cars", "table2"))
  
  names(andr) <- toupper(names(andr))
  expect_equal(names(andr), c("CARS", "TABLE2"))
  
  expect_equal(colnames(andr$CARS), names(cars))
  
  names(andr$CARS) <- c("col1", "col2")
  expect_equal(names(andr$CARS), c("col1", "col2"))
  
  close(andr)
})

test_that("isAndromedaTable", {
  # sqlite version
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  DBI::dbWriteTable(con, "cars", cars)
  db <- dplyr::tbl(con, "cars")
  expect_true(isAndromedaTable(db))
  expect_true(isAndromedaTable(dplyr::mutate(db, a = 1)))
  
  a <- andromeda(cars = cars)
  class(a$cars)
  expect_true(isAndromedaTable(a$cars))
  expect_true(isAndromedaTable(dplyr::mutate(a$cars, a = 1)))
  
  # arrow version
  path <- tempfile()
  arrow::write_feather(cars, path)
  ds <- arrow::open_dataset(path, format = "feather")
  class(ds)
  expect_true(isAndromedaTable(ds))
  expect_true(isAndromedaTable(dplyr::mutate(ds, a = 1)))
})
