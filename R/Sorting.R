# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of Andromeda
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Check if data are sorted by one or more columns
#'
#' @description
#' Checks whether data are sorted by one or more specified columns.
#'
#' @param data            Either a data frame or [`Andromeda`] table.
#' @param columnNames     Vector of one or more column names.
#' @param ascending       Logical vector indicating the data should be sorted ascending or 
#'                        descending according the specified columns.
#'
#' @details
#' This function currently only supports checking for sorting on numeric values.
#'
#' @return
#' `TRUE` or `FALSE`.
#'
#' @examples
#' x <- data.frame(a = runif(1000), b = runif(1000))
#' x <- round(x, digits=2)
#' isSorted(x, c("a", "b"))
#'
#' x <- x[order(x$a, x$b),]
#' isSorted(x, c("a", "b"))
#'
#' x <- x[order(x$a,-x$b),]
#' isSorted(x, c("a", "b"), c(TRUE, FALSE))
#'
#' @export
isSorted <- function(data, columnNames, ascending = rep(TRUE, length(columnNames))){
  UseMethod("isSorted")
}

#' @describeIn isSorted Check if a `data.frame` is sorted by one or more columns
#' @export
isSorted.data.frame <- function(data, columnNames, ascending = rep(TRUE, length(columnNames))){
  return(.isSorted(data, columnNames, ascending))
}

#' @describeIn isSorted Check if an [`Andromeda`] table is sorted by one or more columns
#' @export
isSorted.tbl_dbi <- function(data, columnNames, ascending = rep(TRUE, length(columnNames))) {
  if (nrow(data) > 100000) { #If data is big, first check on a small subset. If that aready fails, we're done
    if (!.isSortedVectorList(data %>% 
                             dplyr::select(columnNames) %>% 
                             head(1000) %>% 
                             dplyr::collect(),
                             ascending = ascending)) {
      return(FALSE)
    }
  }
  Andromeda::batchTest(data,
                       function(batch) {
                         .isSortedVectorList(batch %>% 
                                               dplyr::select(columnNames) %>% 
                                               dplyr::collect(),
                                             ascending = ascending)
                       })
}
