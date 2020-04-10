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

#' Collect in batches
#'
#' @param tbl       An Andromeda table.
#' @param fun       A function that takes a single data frame as argument.
#' @param batchSize Number of rows to fetch at a time.
#'
#' @export
collectBatched <- function(tbl, fun, batchSize = 10000) {
  if (!inherits(tbl, "tbl_dbi")) 
    stop("First argument must be an Andromeda table") 
  if (!is.function(fun)) 
    stop("Second argument must be a function")
  
  # # Open a new read-only connection. Can't read and write at the same time using the same connection
  # connection <- RSQLite::dbConnect(RSQLite::SQLite(), tbl$src$con@dbname, flags = RSQLite::SQLITE_RO)
  # on.exit(RSQLite::dbDisconnect(connection))
  connection <- tbl$src$con
  sql <-  dbplyr::sql_render(tbl, connection)
  result <- RSQLite::dbSendQuery(connection, sql)
  tryCatch({
    while (!RSQLite::dbHasCompleted(result)) {
      batch <- RSQLite::dbFetch(result, n = batchSize)
      fun(batch)
    }
  }, finally = {
    RSQLite::dbClearResult(result)
  })
  invisible(NULL)
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
  
  connection <- tbl$src$con
  tableName <- tbl$ops$x
  if (inherits(data, "data.frame")) {
    
    RSQLite::dbWriteTable(conn = connection,
                          name = tableName,
                          value = data,
                          overwrite = FALSE,
                          append = TRUE)
  } else if (inherits(data, "tbl_dbi")) {

    doBatchedAppend <- function(batch) {
      RSQLite::dbWriteTable(conn = connection,
                            name = tableName,
                            value = batch,
                            overwrite = FALSE,
                            append = TRUE)
    }
    if (isTRUE(all.equal(tbl$src$con, data$src$con))) {
      # Cannot read and write to the same database at the same time. 
      # Create a copy in a temp Andromeda first
      #TODO: use a INSERT INTO TABLE instead
      tempAndromeda <- Andromeda()
      tempAndromeda$table <- data
      on.exit(RSQLite::dbDisconnect(tempAndromeda))
      data <- tempAndromeda$table
    } 
    collectBatched(data, doBatchedAppend)
  }
  invisible(NULL)
}
