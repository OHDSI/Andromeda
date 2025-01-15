# Copyright 2024 Observational Health Data Sciences and Informatics
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

#' Create an index on one or more columns in an Andromeda table
#'
#' @param tbl             An [`Andromeda`] table (or any other 'DBI' table).
#' @param columnNames     A vector of column names (character) on which the index is to be created.
#' @param unique          Should values in the column(s) be enforced to be unique?
#' @param indexName       The name of the index. If not provided, a random name will be generated.
#' 
#' @details
#' Indices can speed up subsequent queries that use the indexed columns, but can take time to create, 
#' and will take additional space on the drive. 
#' 
#' @seealso [listIndices()], [removeIndex()]
#' 
#' @return 
#' Invisibly returns the input table.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#'
#' createIndex(andr$cars, "speed")
#'
#' # Will be faster now that speed is indexed:
#' andr$cars %>%
#'   filter(speed == 10) %>%
#'   collect()
#' 
#' close(andr)
#'
#' @export
createIndex <- function(tbl, columnNames, unique = FALSE, indexName = NULL) {
  if (!inherits(tbl, "tbl_dbi"))
    abort("First argument must be an Andromeda (or DBI) table")
  if (!all(columnNames %in% colnames(tbl)))
    abort(sprintf("Column(s) %s not found in the table", paste(columnNames[!columnNames %in% names(tbl)], collapse = ", ")))
  
  if (is.null(indexName)) {
    indexName <- paste(c("idx_", sample(c(letters, 0:9), 20)), collapse = "")
  }
  
  statement <- sprintf("CREATE %s INDEX %s ON %s(%s);", 
                       if (unique) "UNIQUE" else "", 
                       indexName, 
                       as.character(dbplyr::remote_name(tbl)), 
                       paste(columnNames, collapse = ", "))
  
  DBI::dbExecute(conn = dbplyr::remote_con(tbl), statement = statement)
  invisible(tbl)
}


#' List all indices on an Andromeda table
#'
#' @param tbl             An [`Andromeda`] table (or any other 'DBI' table).
#' 
#' @details
#' Lists any indices that may have been created using the  [createIndex()] function. 
#' 
#' @seealso [createIndex()], [removeIndex()]
#' 
#' @return 
#' Returns a tibble listing the indices, indexed columns, and whether the index is unique.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#'
#' createIndex(andr$cars, "speed")
#'
#' listIndices(andr$cars)
#' # # A tibble: 1 x 5
#' # indexSequenceId indexName                unique columnSequenceId columnName
#' #           <int> <chr>                    <lgl>             <int> <chr>     
#' #1              0 idx_ocy8we9j2i7ld0rshgb4 FALSE                 0 speed           
#' 
#' close(andr)
#'
#' @export
listIndices <- function(tbl) {
  if (!inherits(tbl, "tbl_dbi")) abort("Argument must be an Andromeda (or DBI) table")
  
  tableName <- as.character(dbplyr::remote_name(tbl))
  conn <- dbplyr::remote_con(tbl)
  sql <- sprintf("select * from duckdb_indexes where table_name = '%s';", tableName)
  DBI::dbGetQuery(conn, sql) %>%
    dplyr::as_tibble()
}

#' Removes an index from an Andromeda table
#'
#' @param tbl             An [`Andromeda`] table (or any other 'DBI' table).
#' @param columnNames     A vector of column names (character) on which the index was created. If not
#'                        provided, then the `indexName` argument must be provided.
#' @param indexName       The name of the index. If not provided, the `columnNames` argument must be 
#'                        provided.
#' 
#' @details
#' Remove an index created using the [createIndex()] function. Either the index name or the column 
#' names on which the index was created must be provided. 
#' 
#' @seealso [createIndex()], [listIndices()]
#' 
#' @return 
#' Invisibly returns the input table.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#'
#' createIndex(andr$cars, "speed")
#'
#' # Will be faster now that speed is indexed:
#' andr$cars %>%
#'   filter(speed == 10) %>%
#'   collect()
#'   
#' removeIndex(andr$cars, "speed")
#' 
#' close(andr)
#'
#' @export
removeIndex <- function(tbl, columnNames = NULL, indexName = NULL) {
  if (!inherits(tbl, "tbl_dbi")) 
    abort("First argument must be an Andromeda (or DBI) table")
  
  if (!((is.character(columnNames) && length(columnNames) > 0) || (is.character(indexName) && length(indexName) > 0))) 
      abort("Either columnNames or indexName must be supplied.")
  
  tableName <- as.character(dbplyr::remote_name(tbl))
  connection <- dbplyr::remote_con(tbl)
  indices <- listIndices(tbl)
  
  if (is.character(columnNames) && length(columnNames) > 0) {
    # get the index name that matches the columnNames
    indexName <- indices %>% 
      # filter to indices with the number of columns passed in
      filter(stringr::str_count(.data$sql, ",") == (length(columnNames) - 1)) %>% 
      # get index that matches all column names
      filter(all(stringr::str_detect(.data$sql, columnNames))) %>% 
      pull(.data$index_name)
    
    if (length(indexName) == 0) {
      abort(sprintf("Could not find an index on column(s) %s", paste(columnNames, collapse = ", ")))
    }
  } else {
    for(i in indexName) {
      if (!(indexName %in% indices$index_name)) abort(sprintf("Index with name '%s' not found", i))
    }
  }
  
  for(i in indexName) {
    statement <- sprintf("DROP INDEX IF EXISTS %s;", i)
    DBI::dbExecute(conn = dbplyr::remote_con(tbl), statement = statement)
  }
  invisible(tbl)
}
