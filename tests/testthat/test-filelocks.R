library(testthat)

test_that('Andromeda removes files after batchApply', {
  fn <- function(df) {
    df[, 'rowId'] <- NULL
  }
  
  covariates <- data.frame(rowId = rep(1:nrow(infert),2),
                           covariateId = rep(1:2,each=nrow(infert)),
                           covariateValue = c(infert$spontaneous,infert$induced))
  outcomes <- data.frame(rowId = 1:nrow(infert),
                         y = infert$case)
  #Make sparse:
  covariates <- covariates[covariates$covariateValue != 0,]
  
  andr <- Andromeda::andromeda(outcomes = outcomes, covariates = covariates)
  
  convertData <- function(covariates, outcomes) {
    outcomeRowIds <- dplyr::select(outcomes, "rowId") %>% dplyr::pull()
    covariates <- covariates %>% dplyr::filter(.data$rowId %in% outcomeRowIds)
    covariates <- covariates %>% dplyr::arrange(.data$covariateId, .data$rowId)
    Andromeda::batchApply(covariates, fn)
    return(invisible(NULL))
  }
  
  convertData(andr$covariates, andr$outcomes)
  Andromeda::close(andr)
  
  expect_true(!dir.exists(attr(andr, "path")))
  
})
