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

#' Apply a function to batches of data in an Andromeda table
#'
#' @param tbl       An Andromeda table (or any other DBI table).
#' @param fun       A function where the first argument is a data frame.
#' @param ...       Additional parameters passed to fun.
#' @param batchSize Number of rows to fetch at a time.
#' @param safe      Create a copy of tbl first? Allows writing to the same Andromeda
#'                  as being read from.
#' 
#' @details 
#' This function is similar to the \code{lapply} function, in that it applies a 
#' function to sets of data. In this case, the data is batches of data from an
#' Andromeda table. Each batch will be presented to the function as a data frame.
#'
#' @export
batchApply <- function(tbl, fun, ..., batchSize = 10000, safe = FALSE) {
  if (!inherits(tbl, "tbl_dbi")) 
    stop("First argument must be an Andromeda (or DBI) table") 
  if (!is.function(fun)) 
    stop("Second argument must be a function")
  
  if (safe) {
    tempAndromeda <- andromeda() 
    on.exit(close(tempAndromeda))
    tempAndromeda$tbl <- tbl
    connection <- dbplyr::remote_con(tempAndromeda$tbl)
    sql <-  dbplyr::sql_render(tempAndromeda$tbl, connection)
  } else {
    connection <- dbplyr::remote_con(tbl)
    sql <-  dbplyr::sql_render(tbl, connection)
  }
  result <- DBI::dbSendQuery(connection, sql)
  output <- list()
  tryCatch({
    while (!DBI::dbHasCompleted(result)) {
      batch <- DBI::dbFetch(result, n = batchSize)
      output[[length(output) + 1]] <- do.call(fun, append(list(batch), list(...)))
    }
  }, finally = {
    DBI::dbClearResult(result)
  })
  invisible(output)
}

#' Append to an Andromeda table
#'
#' @param tbl       An Andromeda table. This must be a base table (i.e. it cannot be a query result).
#' @param data      The data to append. This can be either a data.frame or another Andromeda table.
#'
#' @export
appendToTable <- function(tbl, data) {
  if (!inherits(tbl, "tbl_dbi")) 
    stop("First argument must be an Andromeda table") 
  if (!inherits(tbl$ops, "op_base_remote") )
    stop("First argument must be a base table (cannot be a query result)")
  
  connection <- dbplyr::remote_con(tbl)
  tableName <- dbplyr::remote_name(tbl)
  if (inherits(data, "data.frame")) {
    
    RSQLite::dbWriteTable(conn = connection,
                          name = tableName,
                          value = data,
                          overwrite = FALSE,
                          append = TRUE)
  } else if (inherits(data, "tbl_dbi")) {
    if (isTRUE(all.equal(connection, dbplyr::remote_con(data)))) {
      sql <-  dbplyr::sql_render(data, connection)
      sql <- sprintf("INSERT INTO %s %s", tableName, sql)
      RSQLite::dbExecute(connection, sql)
    } else {
      doBatchedAppend <- function(batch) {
        RSQLite::dbWriteTable(conn = connection,
                              name = tableName,
                              value = batch,
                              overwrite = FALSE,
                              append = TRUE)
      }
      batchApply(data, doBatchedAppend)
    }
  }
  invisible(NULL)
}



#' @export
dim.tbl_dbi <- function(x) {
  if (!inherits(x, "tbl_dbi")) 
    stop("Argument must be an Andromeda table") 
  return(c((x %>% dplyr::count() %>%  dplyr::collect())$n, length(dbplyr::op_vars(x))))
}
