library(testthat)

test_that("Saving and loading", {
  andromeda <- andromeda()
  andromeda$table <- iris
  expect_true("table" %in% names(andromeda))
  iris1 <- andromeda$table %>% collect()

  attr(andromeda, "metaData") <- list(x = 1)

  fileName <- tempfile(fileext = ".zip")

  saveAndromeda(andromeda, fileName, maintainConnection = FALSE)

  andromeda2 <- loadAndromeda(fileName)
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
