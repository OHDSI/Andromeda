library(testthat)

test_that("Object creation", {
  andromeda <- andromeda()
  expect_true(isAndromeda(andromeda))
  expect_true(isValidAndromeda(andromeda))

  close(andromeda)
  # expect_error(names(andromeda), "no longer valid")
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
  expect_setequal(names(andromeda), c("cars", "cars2"))
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

  # count <- andromeda$cars %>% count() %>% collect()
  # expect_equal(count$n, 0)

  cars2 <- andromeda$cars %>% collect()
  expect_equal(nrow(cars2), 0)

  # andromeda$iris <- iris
  # andromeda$iris2 <- andromeda$iris %>% filter(Sepal.Length > 100)

  # count2 <- andromeda$iris2 %>% count() %>% collect()
  # expect_equal(count2$n, 0)

  # andromeda2 <- andromeda(iris2 = andromeda$iris2)

  # count3 <- andromeda2$iris2 %>% count() %>% collect()
  # expect_equal(count3$n, 0)

  close(andromeda)
  # close(andromeda2)
})

test_that("file system reference stay consistent with what is on disk", {
  a <- andromeda(cars = cars, iris = iris)
  b <- a
  
  expect_equal(names(a), c("cars", "iris"))
  expect_equal(names(b), c("cars", "iris"))
  
  b$cars <- NULL
  
  expect_equal(names(a), "iris")
  expect_equal(names(b), "iris")
  expect_equal(attr(b, "names"), "iris")
  
  # internal name still exists. I can't see a way to remove it.
  expect_equal(attr(a, "names"), c("cars", "iris")) 
  # The returned value is NULL
  expect_null(a$cars)
  
  # however str(a) shows that the reference is still there
  
  a$iris <- NULL
  expect_equal(names(a), character(0L))
  expect_equal(names(b), character(0L))
  
  close(a)
  close(b)
})

test_that("Object cleanup", {
  andromeda <- andromeda()

  fileName <- attr(andromeda, "path")
  expect_true(file.exists(fileName))

  close(andromeda)
  expect_false(file.exists(fileName))

  andromeda2 <- andromeda()

  fileName <- attr(andromeda2, "path")
  expect_true(file.exists(fileName))

  rm(andromeda2)
  invisible(gc())
  expect_false(file.exists(fileName))
})

test_that("Setting the andromeda temp folder", {
  folder <- tempfile()
  
  withr::with_options(list(andromedaTempFolder = folder), {
    andromeda <- andromeda()
    files <- list.files(folder)
    expect_equal(length(files), 1)
    close(andromeda)
    unlink(folder, recursive = TRUE)
  })
})

test_that("Copying andromeda", {
  andromeda <- andromeda()
  andromeda$cars <- cars

  andromeda2 <- copyAndromeda(andromeda)

  expect_true("cars" %in% names(andromeda2))
  expect_false(attr(andromeda, "path") == attr(andromeda2, "path"))
  close(andromeda)
  close(andromeda2)
})

test_that("Warning when disk space low", {
  skip_if(!.Platform$OS.type %in% c("windows", "unix"))
  
  availableSpace <- tryCatch(getAndromedaTempDiskSpace(), 
                             error = function(e) NA)
  skip_if(is.na(availableSpace))

  andromeda <- andromeda()
  options(warnDiskSpaceThreshold = Inf)
  expect_warning(andromeda$cars <- cars)
  close(andromeda)
  options(warnDiskSpaceThreshold = NULL)
})

# test_that("Get/set Andromeda table/column names works.", {
#   andr <- andromeda()
#   expect_length(names(andr), 0L)
#   
#   andr$cars <- cars
#   andr[["iris"]] <- iris
#   expect_equal(names(andr), c("cars", "iris"))
#   
#   names(andr) <- c("cars", "table2")
#   expect_equal(names(andr), c("cars", "table2"))
#   
#   names(andr) <- toupper(names(andr))
#   expect_equal(names(andr), c("CARS", "TABLE2"))
#   
#   expect_equal(colnames(andr$CARS), names(cars))
#   
#   names(andr$CARS) <- c("col1", "col2")
#   expect_equal(names(andr$CARS), c("col1", "col2"))
#   
#   close(andr)
# })


