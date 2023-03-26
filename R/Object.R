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
#' The `Andromeda` class is an S4 object.
#' 
#' This class provides the ability to work with data objects in R that are too large to fit in memory. Instead, 
#' these objects are stored on disk. This is slower than working from memory, but may be the only viable option. 
#' 
#' @section Tables:
#' An `Andromeda` object has zero, one or more tables. The list of table names can be retrieved using the [`names()`] 
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
#' The `Andromeda` inherits directly from `SQLiteConnection.` As such, it can be used as if it is a `SQLiteConnection`. 
#' [`RSQLite`] is an R wrapper around 'SQLite', a low-weight but very powerful single-user SQL database that can run 
#' from a single file on the local file system.
#' 
#' @name Andromeda-class
#' @aliases Andromeda
#' @seealso [`andromeda()`]
#' @import RSQLite
#' @importClassesFrom DBI DBIObject DBIConnection
#' @export
setClass("Andromeda", contains = "SQLiteConnection")

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
andromeda <- function(...) {
  arguments <- list(...)
  if (length(arguments) > 0) {
    if (is.null(names(arguments)) || any(names(arguments) == ""))
      abort("All arguments must be named")
  }
  andromeda <- .createAndromeda()
  if (length(arguments) > 0) {
    for (name in names(arguments)) {
      andromeda[[name]] <- arguments[[name]]
    }
  }
  return(andromeda)
}

#' Copy Andromeda
#'
#' @param andromeda   The [`Andromeda`] object to copy.
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
  checkIfValid(andromeda)
  newAndromeda <- .createAndromeda()
  RSQLite::sqliteCopyDatabase(andromeda, newAndromeda)
  return(newAndromeda)
}

.createAndromeda <- function() {
  tempFolder <- .getAndromedaTempFolder()
  andromeda <- RSQLite::dbConnect(RSQLite::SQLite(),
                                  tempfile(tmpdir = tempFolder, fileext = ".sqlite"),
                                  extended_types = TRUE)
  class(andromeda) <- "Andromeda"
  attr(class(andromeda),"package") <- "Andromeda"
  finalizer <- function(ptr) {
    # Suppress R Check note:
    missing(ptr)
    close(andromeda)
  }
  reg.finalizer(andromeda@ptr, finalizer, onexit = TRUE)
  RSQLite::dbExecute(andromeda, "PRAGMA journal_mode = OFF") 
  RSQLite::dbExecute(andromeda, sprintf("PRAGMA temp_store_directory = '%s'", tempFolder)) 
  return(andromeda)
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

#' @param object  An [`Andromeda`] object.
#' @export
#' @rdname
#' Andromeda-class
setMethod("show", "Andromeda", function(object) {
  cli::cat_line(pillar::style_subtle("# Andromeda object"))

  if (RSQLite::dbIsValid(object)) {
    cli::cat_line(pillar::style_subtle(paste("# Physical location: ", object@dbname)))
    cli::cat_line("")
    cli::cat_line("Tables:")
    for (name in RSQLite::dbListTables(object)) {
      cli::cat_line(paste0("$",
                           name,
                           " (",
                           paste(RSQLite::dbListFields(object, name), collapse = ", "),
                           ")"))
    }
  } else {
    cli::cli_alert_danger("Connection closed")
  }
  invisible(NULL)
})

#' @param x     An [`Andromeda`] object.
#' @param name  The name of a table in the [`Andromeda`] object.
#' @export
#' @rdname
#' Andromeda-class
setMethod("$", "Andromeda", function(x, name) {
  return(x[[name]])

})

#' @param x     An [`Andromeda`] object.
#' @param name  The name of a table in the [`Andromeda`] object.
#' @param value A data frame, [`Andromeda`] table, or other 'DBI' table.
#' @export
#' @rdname
#' Andromeda-class
setMethod("$<-", "Andromeda", function(x, name, value) {
  x[[name]] <- value
  return(x)
})

#' @param x    An [`Andromeda`] object.
#' @param i    The name of a table in the [`Andromeda`] object.
#' @param value A data frame, [`Andromeda`] table, or other 'DBI' table.
#' @export
#' @rdname
#' Andromeda-class
setMethod("[[<-", "Andromeda", function(x, i, value) {
  checkIfValid(x)
  if (is.null(value)) {
    if (i %in% names(x)) {
      RSQLite::dbRemoveTable(x, i)
    }
  } else if (inherits(value, "data.frame")) {
    .checkAvailableSpace(x)
    RSQLite::dbWriteTable(conn = x, name = i, value = value, overwrite = TRUE, append = FALSE)
  } else if (inherits(value, "tbl_dbi")) {
    .checkAvailableSpace(x)
    if (isTRUE(all.equal(x, dbplyr::remote_con(value)))) {
      sql <- dbplyr::sql_render(value, x)
      if (RSQLite::dbExistsTable(x, i)) {
        # Maybe we're copying data from a table into the same table. So write to temp
        # table first, then drop old table, and rename temp to old name:
        tempName <- paste(sample(letters, 16), collapse = "")
        sql <- sprintf("CREATE TABLE %s AS %s", tempName, sql)
        RSQLite::dbExecute(x, sql)
        RSQLite::dbRemoveTable(x, i)
        sql <- sprintf("ALTER TABLE %s RENAME TO %s;", tempName, i)
        RSQLite::dbExecute(x, sql)
      } else {
        sql <- sprintf("CREATE TABLE %s AS %s", i, sql)
        RSQLite::dbExecute(x, sql)
      }
    } else {
      if (RSQLite::dbExistsTable(x, i)) {
        RSQLite::dbRemoveTable(x, i)
      }
      doBatchedAppend <- function(batch) {
        RSQLite::dbWriteTable(conn = x, name = i, value = batch, overwrite = FALSE, append = TRUE)
        return(TRUE)
      }
      dummy <- batchApply(value, doBatchedAppend)
      if (length(dummy) == 0) {
        RSQLite::dbWriteTable(conn = x, name = i, value = dplyr::collect(value), overwrite = FALSE, append = TRUE)
      }
    }
  } else {
    abort("Table must be a data frame or dplyr table")
  }
  x
})

#' @param x    An [`Andromeda`] object.
#' @param i    The name of a table in the [`Andromeda`] object.
#' @export
#' @rdname
#' Andromeda-class
setMethod("[[", "Andromeda", function(x, i) {
  checkIfValid(x)
  if (RSQLite::dbExistsTable(x, i)) {
    return(dplyr::tbl(x, i))
  } else {
    return(NULL)
  }
})

#' names
#'
#' @description
#' Show the names of the tables in an Andromeda object.
#'
#' @param x    An [`Andromeda`] object.
#' 
#' @return 
#' A vector of names.
#'
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#'
#' names(andr)
#' # [1] 'cars' 'iris'
#'
#' close(andr)
#'
#' @rdname
#' Andromeda-class
#' 
#' @export
setMethod("names", "Andromeda", function(x) {
  checkIfValid(x)
  RSQLite::dbListTables(x)
})

#' Set table names in an Andromeda object
#' 
#' names(andromedaObject) must be set to a character vector with length equal to the number of
#' tables in the andromeda object (i.e. length(andromedaObject)). The user is 
#' responsible for setting valid table names (e.g. not using SQL keywords or numbers as names)
#' This function treats Andromeda table names as case insensitive so if the only difference 
#' between the new names and old names is the case then the names will not be changed.
#'
#' @param x An Andromeda object
#' @param value A character vector with the same length as the number of tables in x
#'
#' @export
#'
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#' names(andr) <- c("CARS", "IRIS")
#' names(andr)
#' # [1] "CARS" "IRIS"
#' close(andr)
#' 
setMethod("names<-", "Andromeda", function(x, value) {
  checkIfValid(x)
  nm <- names(x)
  if(!is.character(value) || !(length(nm) == length(value))) {
    rlang::abort("New names must be a character vector with the same length as names(x).")
  }
  
  for(i in seq_along(nm)) {
    if((nm[i] != value[i]) & (tolower(nm[i]) == tolower(value[i]))) {
      # Handle case when names differ only by case
      DBI::dbExecute(x, sprintf("ALTER TABLE %s RENAME TO %s;", nm[i], paste0(nm[i], "0")))
      DBI::dbExecute(x, sprintf("ALTER TABLE %s RENAME TO %s;", paste0(nm[i], "0"), value[i]))
    } else if(nm[i] != value[i]) {
      DBI::dbExecute(x, sprintf("ALTER TABLE %s RENAME TO %s;", nm[i], value[i]))
    }
  }
  
  invisible(x)
})


#' Get the column names of an Andromeda table
#'
#' @param x An table in an Andromeda object
#'
#' @return A character vector of column names
#' @export
#'
#' @examples
#' andr <- andromeda(cars = cars)
#' names(andr$cars)
#' # [1] "speed" "dist"
#' close(andr)
names.tbl_Andromeda <- function(x) {
  colnames(x)
}

#' Set column names of an Andromeda table
#'
#' @param x A reference to a table in an andromeda object. (see examples)
#' @param value A character vector of new names that must have length equal to the number of columns in the table.
#'
#' @export
#'
#' @examples
#' andr <- andromeda(cars = cars)
#' names(andr$cars) <- toupper(names(andr$cars))
#' names(andr$cars)
#' # [1] "SPEED" "DIST" 
#' close(andr)
"names<-.tbl_Andromeda" <- function(x, value) {
  tableName <- dbplyr::remote_name(x)
  connection <- dbplyr::remote_con(x)
  nm <- names(x)
  if(!is.character(value) || !(length(nm) == length(value))) {
    rlang::abort("New names must be a character vector with the same length as names(x).")
  }
  
  idx <- nm != value
  if (any(idx)) {
    sql <- sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tableName, nm[idx], value[idx])
    lapply(sql, function(statement) DBI::dbExecute(connection, statement))
  }
  invisible(x)
}


#' @param x    An [`Andromeda`] object.
#' @export
#' @rdname
#' Andromeda-class
setMethod("length", "Andromeda", function(x) {
  length(names(x))
})

#' Check whether an object is an Andromeda object
#'
#' @param x   The object to check.
#'
#' @details
#' Checks whether an object is an Andromeda object.
#'
#' @return
#' A logical value.
#'
#' @export
isAndromeda <- function(x) {
  return(inherits(x, "Andromeda"))
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
  if(!isAndromeda(x)) rlang::abort(paste(deparse(substitute(x)), "is not an Andromeda object."))
  return(RSQLite::dbIsValid(x))
}

#' @param con    An [`Andromeda`] object.
#' @param ...	   Included for compatibility with generic `close()` method.
#' @export
#' @rdname
#' Andromeda-class
setMethod("close", "Andromeda", function(con, ...) {
  fileName <- con@dbname
  if (RSQLite::dbIsValid(con)) {
    RSQLite::dbDisconnect(con)
  }
  if (file.exists(fileName)) {
    unlink(fileName)
  }
})

checkIfValid <- function(x) {
  if (!isValidAndromeda(x))
    abort("Andromeda object is no longer valid. Perhaps it was saved without maintainConnection = TRUE, or R has been restarted?")
}

#' Is the object an Andromeda table?
#'
#' @param tbl A reference to an Andromeda table
#'
#' @return TRUE or FALSE
#' @export
#'
#' @examples
#' \dontrun{
#' andr <- andromeda(cars = cars)
#' isAndromedaTable(andr$cars)
#' close(andr)
#' }
isAndromedaTable <- function(tbl) {
  return(
    inherits(tbl, "FileSystemDataset") ||
    inherits(tbl, "arrow_dplyr_query") ||
    inherits(tbl, "tbl_Andromeda") ||
    inherits(tbl, "tbl_SQLiteConnection")
  )
}
