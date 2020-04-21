library("testthat")
library("Andromeda")

context("test-isSorted.R")

test_that("isSorted data.frame", {
  x <- data.frame(a = runif(1000),b = runif(1000))
  x <- round(x,digits=2)
  expect_false(isSorted(x,c("a","b"),c(TRUE,FALSE)))
  x <- x[order(x$a,x$b),]

  expect_true(isSorted(x,c("a","b")))
  expect_false(isSorted(x,c("a","b"),c(TRUE,FALSE)))

  x <- x[order(x$a,-x$b),]
  expect_true(isSorted(x,c("a","b"),c(TRUE,FALSE)))
  expect_false(isSorted(x,c("a","b")))
})

test_that("isSorted Andromeda", {
#   x <- data.frame(a = runif(20000000),b = runif(20000000)) # Takes too much time for a unit-test
  x <- data.frame(a = runif(200),b = runif(200))
  x <- round(x,digits=2)
  andr <- andromeda(x = x)
  expect_false(isSorted(andr$x,c("a","b"),c(TRUE,FALSE)))

  andr$xSorted <- andr$x %>% dplyr::arrange(a, b)
  expect_true(isSorted(andr$xSorted,c("a","b")))
  expect_false(isSorted(andr$xSorted,c("a","b"),c(TRUE,FALSE)))
  
  andr$xSorted2 <- andr$xSorted %>% mutate(minb = 0 - b) %>% arrange(a, minb)
  expect_true(isSorted(andr$xSorted2,c("a","b"),c(TRUE,FALSE)))
  expect_false(isSorted(andr$xSorted2,c("a","b")))
})
