library(testthat)

test_that("Saving and loading", {
  andromeda <- andromeda()
  andromeda$table <- iris
  expect_true("table" %in% names(andromeda))
  iris1 <- andromeda$table %>% collect()

  attr(andromeda, "metaData") <- list(x = 1)

  fileName <- tempfile(fileext = ".zip")
  
  # saveAndromeda writes to stdout which we want to supress when testing
  expect_error(
    capture.output(
      saveAndromeda(andromeda, fileName, maintainConnection = FALSE)
    ),
  NA)
  
  expect_error(saveAndromeda(andromeda, fileName), "closed")

  expect_error(andromeda2 <- loadAndromeda(fileName), NA)
  expect_true("table" %in% names(andromeda2))

  iris2 <- andromeda2$table %>% collect()

  expect_equivalent(iris1, iris2)
  expect_false(is.null(attr(andromeda, "metaData")))
  expect_equal(attr(andromeda, "metaData")$x, 1)

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
})


# test_that(".checkAvailableSpace works", {
#   space <- getAndromedaTempDiskSpace()
#   if (!is.na(space)) {
#     # rJava is installed, so we can test:
#     
#     oldOption <- getOption("warnDiskSpaceThreshold")
#     options(warnDiskSpaceThreshold = 1e15)
#     expect_warning(.checkAvailableSpace(), "Low disk space")
#     # Checking the same location again should not produce a warning
#     expect_warning(.checkAvailableSpace(), NA)
#     options(warnDiskSpaceThreshold = oldOption)
#   }
# })
