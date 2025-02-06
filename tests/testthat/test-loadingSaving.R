library(testthat)

test_that("Saving and loading", {
  andromeda <- andromeda()
  andromeda$iris <- iris
  expect_true("iris" %in% names(andromeda))
  iris1 <- andromeda$iris %>% collect()

  attr(andromeda, "metaData") <- list(x = 1)

  fileName <- tempfile(fileext = ".zip")

  # saveAndromeda writes to stdout which we want to suppress when testing
  # Note: expect_error(expression, NA) => expect no error
  expect_error(
    capture.output(
      saveAndromeda(andromeda, fileName, maintainConnection = FALSE)
    ),
  NA)

  expect_error(saveAndromeda(andromeda, fileName), "closed")
  expect_error(saveAndromeda(andromeda, fileName, overwrite = FALSE), "already exists")

  expect_error(andromeda2 <- loadAndromeda(fileName), NA)
  expect_true("iris" %in% names(andromeda2))

  iris2 <- andromeda2$iris %>% collect()

  expect_equivalent(iris1, iris2)
  expect_false(is.null(attr(andromeda, "metaData")))
  expect_equal(attr(andromeda, "metaData")$x, 1)

  expect_error(
    capture.output(
      saveAndromeda(andromeda2, fileName, maintainConnection = TRUE, overwrite = TRUE)
    ),
    NA)

  expect_error(checkIfValid(andromeda2), NA)
  close(andromeda2)
  unlink(fileName)
})


test_that("Object cleanup when loading and saving", {
  andromeda <- andromeda()
  andromeda$cars <- cars

  expect_true(file.exists(andromeda@dbname))

  fileName <- tempfile(fileext = ".zip")
  saveAndromeda(andromeda, fileName, maintainConnection = FALSE)

  expect_false(file.exists(andromeda@dbname))

  andromeda2 <- loadAndromeda(fileName)
  internalFileName <- andromeda2@dbname

  expect_true(file.exists(internalFileName))

  rm(andromeda2)
  invisible(gc())
  expect_false(file.exists(internalFileName))
  unlink(fileName)
})

test_that("saveAndromeda handles bad file paths and tilde expansion", {
  andromeda <- andromeda(cars = cars)
  expect_error(saveAndromeda(andromeda, "/some/non/exist/ant/path.zip"))
  close(andromeda)

  expect_error(loadAndromeda("/some/non/exist/ant/path.zip"), "does not exist")

})

test_that("saveAndromeda perfroms tilde expansion", {
  skip_if(!dir.exists("~"))
  andromeda <- andromeda(cars = cars)
  expect_error(saveAndromeda(andromeda, "~/andromedatestfile0010101011.zip"), NA)
  unlink("~/andromedatestfile0010101011.zip")
})


test_that("getAndromedaTempDiskSpace works", {
  space <- getAndromedaTempDiskSpace()
  expect_true(is.na(space) || (is.numeric(space) && space > 0))

  andromeda <- andromeda(cars = cars)
  space <- getAndromedaTempDiskSpace(andromeda)
  expect_true(is.na(space) || (is.numeric(space) && space > 0))

  expect_error(getAndromedaTempDiskSpace(cars), "Andromeda argument must be of type 'Andromeda'")
})


test_that(".checkAvailableSpace works", {
  space <- getAndromedaTempDiskSpace()
  if (!is.na(space)) {
    # rJava is installed, so we can test:

    oldOption <- getOption("warnDiskSpaceThreshold")
    options(warnDiskSpaceThreshold = 1e30)
    expect_warning(.checkAvailableSpace(), "Low disk space")
    # Checking the same location again should not produce a warning
    expect_warning(.checkAvailableSpace(), NA)
    options(warnDiskSpaceThreshold = oldOption)
  }
})
