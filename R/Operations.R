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
#' @param tbl             An [`Andromeda`] table (or any other 'DBI' table).
#' @param fun             A function where the first argument is a data frame.
#' @param ...             Additional parameters passed to fun.
#' @param batchSize       Number of rows to fetch at a time.
#' @param progressBar     Show a progress bar?
#' @param safe            Create a copy of tbl first? Allows writing to the same Andromeda as being 
#'                        read from.
#'
#' @details
#' This function is similar to the [`lapply()`] function, in that it applies a function to sets of
#' data. In this case, the data is batches of data from an [`Andromeda`] table. Each batch will be
#' presented to the function as a data frame.
#' 
#' @seealso [groupApply()]
#' 
#' @return 
#' Invisibly returns a list of objects, where each object is the output of the user-supplied function
#' applied to a batch
#'
#' @examples
#' andr <- andromeda(cars = cars)
#'
#' fun <- function(x) {
#'   return(nrow(x))
#' }
#'
#' result <- batchApply(andr$cars, fun, batchSize = 25)
#'
#' result
#' # [[1]] 
#' # [1] 25 
#' # 
#' # [[2]] 
#' # [1] 25
#'
#' close(andr)
#'
#' @export
batchApply <- function(tbl, fun, ..., batchSize = 100000, progressBar = FALSE, safe = FALSE) {
  if (!inherits(tbl, "tbl_dbi"))
    abort("First argument must be an Andromeda (or DBI) table")
  if (!is.function(fun))
    abort("Second argument must be a function")
  
  if (safe) {
    tempAndromeda <- andromeda()
    on.exit(close(tempAndromeda))
    tempAndromeda$tbl <- tbl
    connection <- dbplyr::remote_con(tempAndromeda$tbl)
    sql <- dbplyr::sql_render(tempAndromeda$tbl, connection)
  } else {
    connection <- dbplyr::remote_con(tbl)
    sql <- dbplyr::sql_render(tbl, connection)
  }
  
  output <- list()
  if (progressBar) {
    pb <- txtProgressBar(style = 3)
    totalRows <- tbl %>% count() %>% pull()
    completedRows <- 0
  }
  result <- DBI::dbSendQuery(connection, sql)
  tryCatch({
    while (!DBI::dbHasCompleted(result)) {
      batch <- DBI::dbFetch(result, n = batchSize)
      output[[length(output) + 1]] <- do.call(fun, append(list(batch), list(...)))
      if (progressBar) {
        completedRows <- completedRows + nrow(batch)
        setTxtProgressBar(pb, completedRows/totalRows)
      }
    }
  }, finally = {
    DBI::dbClearResult(result)
    if (progressBar) {
      close(pb)
    }
  })
  invisible(output)
}

#' Apply a function to groups of data in an Andromeda table
#'
#' @param tbl             An [`Andromeda`] table (or any other 'DBI' table).
#' @param groupVariable   The variable to group by
#' @param fun             A function where the first argument is a data frame.
#' @param ...             Additional parameters passed to fun.
#' @param batchSize       Number of rows fetched from the table at a time. This is not the number of
#'                        rows to which the function will be applied. Included mostly for testing
#'                        purposes.
#' @param progressBar     Show a progress bar?
#' @param safe            Create a copy of `tbl` first? Allows writing to the same Andromeda as being
#'                        read from.
#'
#' @details
#' This function applies a function to groups of data. The groups are identified by unique values of
#' the `groupVariable`, which must be a variable in the table.
#'
#' @seealso [batchApply()]
#'
#' @return
#' Invisibly returns a list of objects, where each object is the output of the user-supplied function
#' applied to a group.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#'
#' fun <- function(x) {
#'   return(tibble::tibble(speed = x$speed[1], meanDist = mean(x$dist)))
#' }
#'
#' result <- groupApply(andr$cars, "speed", fun)
#' result <- bind_rows(result)
#' result
#' # # A tibble: 19 x 2 
#' # speed meanDist 
#' # <dbl> <dbl> 
#' # 1 4 6 
#' # 2 7 13 
#' # 3 8 16 
#' # ...
#'
#' close(andr)
#'
#' @export
groupApply <- function(tbl, groupVariable, fun, ..., batchSize = 100000, progressBar = FALSE, safe = FALSE) {
  if (!groupVariable %in% colnames(tbl))
    abort(sprintf("'%s' is not a variable in the table", groupVariable))
  
  env <- new.env()
  assign("output", list(), envir = env)
  wrapper <- function(data, userFun, groupVariable, env, ...) {
    groups <- split(data, data[groupVariable])
    if (!is.null(env$groupValue) && groups[[1]][1, groupVariable] == env$groupValue) {
      groups[[1]] <- bind_rows(groups[[1]], env$groupData)
    }
    if (length(groups) > 1) {
      results <- lapply(groups[1:(length(groups) - 1)], userFun, ...)
      env$output <- append(env$output, results)
    }
    env$groupData <- groups[[length(groups)]]
    env$groupValue <- groups[[length(groups)]][1, groupVariable]
  }
  batchApply(tbl = tbl %>% arrange(rlang::sym(groupVariable)),
             fun = wrapper,
             userFun = fun,
             env = env,
             groupVariable = groupVariable,
             ...,
             batchSize = batchSize,
             progressBar = progressBar,
             safe = safe)
  output <- env$output
  if (!is.null(env$groupData)) {
    output[[length(output) + 1]] <- fun(env$groupData, ...)
    names(output)[length(output)] <- as.character(env$groupValue)
  }
  rm(env)
  invisible(output)
}

#' Append to an Andromeda table
#'
#' @param tbl    An [`Andromeda`] table. This must be a base table (i.e. it cannot be a query result).
#' @param data   The data to append. This can be either a data.frame or another Andromeda table.
#'
#' @description
#' Append a data frame, Andromeda table, or result of a query on an [`Andromeda`] table to an existing
#' [`Andromeda`] table. 
#' 
#' If data from another [`Andromeda`] is appended, a batch-wise copy process is used, which will be slower
#' than when appending data from within the same [`Andromeda`] object.
#' 
#' **Important**: columns are appended based on column name, not on column order. The column names should
#' therefore be identical (but not necessarily in the same order).
#' 
#' @return 
#' Returns no value. Executed for the side-effect of appending the data to the table.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#' nrow(andr$cars)
#' # [1] 50
#'
#' appendToTable(andr$cars, cars)
#' nrow(andr$cars)
#' # [1] 100
#'
#' appendToTable(andr$cars, andr$cars %>% filter(speed > 10))
#' nrow(andr$cars)
#' # [1] 182
#'
#' close(andr)
#'
#' @export
appendToTable <- function(tbl, data) {
  if (!inherits(tbl, "tbl_dbi"))
    abort("First argument must be an Andromeda table")
  if (!inherits(tbl$ops, "op_base_remote"))
    abort("First argument must be a base table (cannot be a query result)")
  
  connection <- dbplyr::remote_con(tbl)
  .checkAvailableSpace(connection)
  tableName <- as.character(dbplyr::remote_name(tbl))
  if (inherits(data, "data.frame")) {
    
    duckdb::dbWriteTable(conn = connection,
                         name = tableName,
                         value = data,
                         overwrite = FALSE,
                         append = TRUE)
  } else if (inherits(data, "tbl_dbi")) {
    if (isTRUE(all.equal(connection, dbplyr::remote_con(data)))) {
      sql <- dbplyr::sql_render(select(data, all_of(colnames(tbl))), connection)
      sql <- sprintf("INSERT INTO %s %s", tableName, sql)
      DBI::dbExecute(connection, sql)
    } else {
      doBatchedAppend <- function(batch) {
        duckdb::dbWriteTable(conn = connection,
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


#' Apply a boolean test to batches of data in an Andromeda table and terminate early
#'
#' @param tbl         An [`Andromeda`] table (or any other 'DBI' table).
#' @param fun         A function where the first argument is a data frame and returns a logical value.
#' @param ...         Additional parameters passed to `fun`.
#' @param batchSize   Number of rows to fetch at a time.
#'
#' @details
#' This function applies a boolean test function to sets of
#' data and terminates at the first `FALSE`.
#' 
#' @return 
#' Returns `FALSE` if any of the calls to the user-supplied function returned `FALSE`, else returns `TRUE`.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#'
#' fun <- function(x) {
#'   is.unsorted(x %>% select(speed) %>% collect())   
#' }
#'
#' result <- batchTest(andr$cars, fun, batchSize = 25)
#'
#' result
#' # [1] FALSE 
#'
#' close(andr)
#'
#' @export
batchTest <- function(tbl, fun, ..., batchSize = 100000) {
  if (!inherits(tbl, "tbl_dbi"))
    abort("First argument must be an Andromeda (or DBI) table")
  if (!is.function(fun))
    abort("Second argument must be a function")
  
  connection <- dbplyr::remote_con(tbl)
  sql <- dbplyr::sql_render(tbl, connection)
  
  result <- DBI::dbSendQuery(connection, sql)
  output <- TRUE
  tryCatch({
    while (!DBI::dbHasCompleted(result) && output) {
      batch <- DBI::dbFetch(result, n = batchSize)
      output <- all(do.call(fun, append(list(batch), list(...))))
    }
  }, finally = {
    DBI::dbClearResult(result)
  })
  return(output)
}

#' Restore dates 
#' 
#' @description 
#' This function has been deprecated since Andromeda v0.5 preserves dates.
#'
#' @param x  A numeric vector representing dates.
#' 
#' @seealso [restorePosixct()]
#'
#' @return
#' A vector of type `Date`.
#' 
#' @examples
#' myData <- data.frame(startDate = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")))
#' andr <- andromeda(myData = myData)
#' 
#' andr$myData %>% 
#'   collect() %>%
#'   mutate(startDate = restoreDate(startDate))
#' # # A tibble: 3 x 1
#' # startDate 
#' # <date>    
#' # 1 2000-01-01
#' # 2 2001-01-31
#' # 3 2004-12-31
#' 
#' close(andr)
#' 
#' @export
restoreDate <- function(x) { 
  rlang::warn("restoreDate has been deprecated since Andromeda v0.5 preserves dates.\nPlease do not use this function.")
  if(inherits(x, "Date")) {
    rlang::warn("Input to restoreDate is already a Date.")
    return(x)
  } else {
    return(as.Date(x, origin = "1970-01-01"))
  }
}

#' Restore timestamps
#' 
#' @description 
#' This function has been deprecated since Andromeda v0.5 preserves POSIXct datetimes.
#'
#' @param x  A numeric vector representing timestamps
#' 
#' @seealso [restoreDate()]
#'
#' @return
#' A vector of type `POSIXct`.
#' 
#' @examples
#' myData <- data.frame(startTime = as.POSIXct(c("2000-01-01 10:00", 
#'                                               "2001-01-31 11:00", 
#'                                               "2004-12-31 12:00")))
#' andr <- andromeda(myData = myData)
#' 
#' andr$myData %>% 
#'   collect() %>%
#'   mutate(startTime = restorePosixct(startTime))
#' # # A tibble: 3 x 1
#' # startTime          
#' # <dttm>             
#' # 1 2000-01-01 10:00:00
#' # 2 2001-01-31 11:00:00
#' # 3 2004-12-31 12:00:00
#' 
#' close(andr)
#' 
#' @export
restorePosixct <- function(x) { 
  rlang::warn("restorePosixct has been deprecated since Andromeda v0.5 preserves datetimes.\nPlease do not use this function.")
  if(inherits(x, "POSIXct")) {
    rlang::warn("Input to restorePosixct is already Posixct")
    return(x)
  } else {
    return(as.POSIXct(x, origin = "1970-01-01"))
  }
}
