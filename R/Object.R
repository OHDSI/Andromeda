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

#' The Andromeda class
#' 
#' @description 
#' The `Andromeda` class is an S3 object.
#' 
#' This class provides the ability to work with data objects in R that are too large to fit in memory. Instead, 
#' these objects are stored on disk. This is slower than working from memory, but may be the only viable option. 
#' The Andromeda object is a thin wrapper around a dm object from the 
#' \href{https://krlmlr.github.io/dm/articles/dm.html}{(data models) package} but is restricted to the duckdb database
#' backend.
#' 
#' @section Tables:
#' An `Andromeda` object can have zero, one, or more tables. The list of table names can be retrieved using the [`names()`] 
#' method. Tables can be accessed using the dollar sign syntax, e.g. `andromeda$myTable`, or double-square-bracket 
#' syntax, e.g. `andromeda[["myTable"]]`
#'
#' 
#' @section Permanence:
#' 
#' To mimic the behavior of in-memory objects, when working with data in `Andromeda` the data is stored in a 
#' temporary location on the disk. You can modify the data as you can see fit, and when needed can save the data 
#' to a permanent location. Later this data can be loaded to a temporary location again and be read and modified, 
#' while keeping the saved data as is.
#' 
#' @section Inheritance:
#' 
#' The `Andromeda` inherits directly from `dm` As such, it can be used as if it is a `SQLiteConnection`. 
#' [`duckdb`] is an R wrapper around 'duckdb', a low-weight but very powerful single-user SQL database similar to 
#' SQLite that can run from a single file on the local file system.
#' 
#' @name Andromeda-class
#' @aliases Andromeda
#' @seealso [`andromeda()`]

#' Create an Andromeda object
#'
#' @description
#' By default the `Andromeda` object is created in the systems temporary file location. You can override
#' this by specifying a folder using `options(andromedaTempFolder = "c:/andromedaTemp")`, where
#' `"c:/andromedaTemp"` is the folder to create the Andromeda objects in.
#'
#' @param ...   Named objects. See details for what objects are valid. If no objects are provided, an
#'              empty Andromeda is returned.
#'
#' @details
#' Valid objects are data frames, `Andromeda` tables, or any other [`dplyr`] table.
#' 
#' @return 
#' Returns an [`Andromeda`] object.
#'
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#'
#' names(andr)
#' # [1] 'cars' 'iris'
#'
#' andr$cars %>% filter(speed > 10) %>% collect()
#' # # A tibble: 41 x 2 
#' # speed dist 
#' # <dbl> <dbl> 
#' # 1 11 17 
#' # ...
#'
#' close(andr)
#' 
#' @rdname andromeda_constructor
#'
#' @export
andromeda <- function(..., options) { 
  arguments <- list(...)
  if (length(arguments) > 0) {
    if (is.null(names(arguments)) || any(names(arguments) == ""))
      rlang::abort("All arguments must be named")
  }
  
  x <- .createAndromeda()
  
  if (length(arguments) > 0) {
      for (name in names(arguments)) {
        print(name)
        x[[name]] <- arguments[[name]]
      }
  }
  return(x)
}

#' @param x    An [`Andromeda`] object.
#' @param i    The name of a table in the [`Andromeda`] object.
#' @param value A data frame, [`Andromeda`] table, or other 'DBI' table.
#' @export
#' @rdname
#' Andromeda-class
# setMethod("[[<-", "Andromeda", function(x, i, value) {
"[[<-.Andromeda" <- function(x, i, value) { 
  # checkIfValid(x)
  con <- attr(x, "conn_ref")
  if (is.null(value)) {
    if (i %in% names(x)) {
      duckdb::dbRemoveTable(con, i)
    }
  } else if (inherits(value, "data.frame")) {
    # .checkAvailableSpace(x)
    duckdb::dbWriteTable(conn = con, name = i, value = value, overwrite = TRUE, append = FALSE)
  } else if (inherits(value, "tbl_dbi")) {
    # .checkAvailableSpace(x)
    if (isTRUE(all.equal(con, dbplyr::remote_con(value)))) {
      sql <- dbplyr::sql_render(value, con)
      if (duckdb::dbExistsTable(con, i)) {
        # Maybe we're copying data from a table into the same table. So write to temp
        # table first, then drop old table, and rename temp to old name:
        tempName <- paste(sample(letters, 16), collapse = "")
        sql <- sprintf("CREATE TABLE %s AS %s", tempName, sql)
        DBI::dbExecute(con, sql)
        duckdb::dbRemoveTable(x, i)
        sql <- sprintf("ALTER TABLE %s RENAME TO %s;", tempName, i)
        DBI::dbExecute(con, sql)
      } else {
        sql <- sprintf("CREATE TABLE %s AS %s", i, sql)
        DBI::dbExecute(con, sql)
      }
    } else {
      if (duckdb::dbExistsTable(x, i)) {
        duckdb::dbRemoveTable(x, i)
      }
      doBatchedAppend <- function(batch) {
        duckdb::dbWriteTable(conn = x, name = i, value = batch, overwrite = FALSE, append = TRUE)
        return(TRUE)
      }
      dummy <- batchApply(value, doBatchedAppend)
      if (length(dummy) == 0) {
        duckdb::dbWriteTable(conn = x, name = i, value = dplyr::collect(value), overwrite = FALSE, append = TRUE)
      }
    }
  } else {
    abort("Table must be a data frame or dplyr table")
  }
  x <- dm::dm_add_tbl(x, !!i := dm::tbl(con, i))
  # restore andromeda attributes
  class(x) <- c("Andromeda", class(x))
  attr(class(x), "package") <- "Andromeda" 
  attr(x, "dbname") <- con@driver@dbdir
  attr(x, "conn_ref") <- con
  x
}



#' Copy Andromeda
#'
#' @param andromeda   The [`Andromeda`] object to copy.
#' @param dbname The location where the new Andromeda object will be stored on disc. Defaults to temp directory.
#'
#' @description
#' Creates a complete copy of an [`Andromeda`] object. Object attributes are not copied.
#'
#' @return
#' The copied [`Andromeda`] object.
#'
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#'
#' andr2 <- copyAndromeda(andr)
#'
#' names(andr2)
#' # [1] 'cars' 'iris'
#'
#' close(andr)
#' close(andr2)
#'
#' @export
copyAndromeda <- function(andromeda) {
  # checkIfValid(andromeda)
  x <- .createAndromeda()
  dm::copy_dm_to(attr(x, "conn_ref"), andromeda)
}


# By default .createAndromeda will create a new duckdb instance in a temp folder. 
# However it can also use an existing duckdb file.
.createAndromeda <- function(dbname = tempfile(tmpdir = .getAndromedaTempFolder(), fileext = ".duckdb")) {
  dbname = tempfile(tmpdir = .getAndromedaTempFolder(), fileext = ".duckdb")
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = dbname)
  
  x <- dm::dm_from_src(con, learn_keys = FALSE)
  class(x) <- c("Andromeda", class(x))
  attr(class(x), "package") <- "Andromeda" # TODO why do we need this?
  attr(x, "dbname") <- con@driver@dbdir
  attr(x, "conn_ref") <- con # TODO support connection to andromeda through DBI or database connector interface.
  # finalizer <- function(conn_ref) {
  #   # Suppress R Check note:
  #   missing(conn_ref) # TODO what is the purpose of this line?
  #   close(andromeda)
  # }
  # reg.finalizer(attr(andromeda, "conn_ref"), finalizer, onexit = TRUE)
  return(x)
}

.getAndromedaTempFolder <- function() {
  tempFolder <- getOption("andromedaTempFolder")
  if (is.null(tempFolder)) {
    tempFolder <- tempdir()
  } else {
    tempFolder <- path.expand(tempFolder)
    if (!file.exists(tempFolder)) {
      dir.create(tempFolder, recursive = TRUE)
    }
  }
  return(tempFolder)
}




checkIfValid <- function(x) {
  if (!isValidAndromeda(x))
    abort("Andromeda object is no longer valid. Perhaps it was saved without maintainConnection = TRUE, or R has been restarted?")
}


#' Check whether an Andromeda object is still valid
#'
#' @param x   The Andromeda object to check.
#'
#' @details
#' Checks whether an Andromeda object is still valid, or whether it has been closed.
#'
#' @return
#' A logical value.
#'
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#'
#' isValidAndromeda(andr)
#' # TRUE
#'
#' close(andr)
#'
#' isValidAndromeda(andr)
#' # FALSE
#'
#' @export
isValidAndromeda <- function(x) {
  return(duckdb::dbIsValid(attr(x, "conn_ref")))
}