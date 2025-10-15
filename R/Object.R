# Copyright 2025 Observational Health Data Sciences and Informatics
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
#' The `Andromeda` inherits directly from `duckdb_connection` As such, it can be used as if it is a `duckdb_connection`. 
#' The `duckdb`package is an R wrapper around 'duckdb', a low-weight but powerful single-user SQL database that can run 
#' from a single file on the local file system.
#' 
#' @name Andromeda-class
#' @aliases Andromeda
#' @seealso [`andromeda()`]
#' @import duckdb
#' @importClassesFrom DBI DBIObject DBIConnection
#' @importClassesFrom duckdb duckdb_connection
#' @export
setClass("Andromeda", slots = c("dbname" = "character"), contains = "duckdb_connection")

#' Create an Andromeda object
#'
#' @description
#' By default the `Andromeda` object is created in the systems temporary file location. You can override
#' this by specifying a folder using `options(andromedaTempFolder = "c:/andromedaTemp")`, where
#' `"c:/andromedaTemp"` is the folder to create the Andromeda objects in.
#' 
#' Although in general Andromeda is well-behaved in terms of memory usage, it can consume a lot of 
#' memory for specific operations such as sorting and aggregating. By default the memory usage is 
#' limited to 75% of the physical memory. However it is possible to set another limit by using
#' `options(andromedaMemoryLimit = 2.5)`, where `2.5` is the number of GB to use at most. One GB is 
#' 1,000,000,000 bytes.
#' 
#' Similarly, by default Andromeda will use all available CPU cores when needed. The `andromedaThreads` 
#' option controls the maximum number of threads Andromeda is allowed to use.
#' 
#' @param ...   Named objects. See details for what objects are valid. If no objects are provided, an
#'              empty Andromeda is returned.
#' @param options A named list of options. Currently the only supported option is 'threads' (see example). 
#'                All other options are ignored.
#'
#' @details
#' Valid objects are data frames, `Andromeda` tables, or any other `dplyr` table.
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
#' # Use multiple threads for queries
#' andr <- andromeda(cars = cars, iris = iris, options = list(threads = 8))
#' 
#' 
#' @rdname andromeda_constructor
#'
#' @export
andromeda <- function(..., options = list()) {
  arguments <- list(...)
  if (length(arguments) > 0) {
    if (is.null(names(arguments)) || any(names(arguments) == ""))
      abort("All arguments must be named")
  }
  andromeda <- .createAndromeda(options = options)
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
#' @param options A list containing Andromeda options. Currently the only supported option is 'threads'.
#'     Setting `options = list(threads = 10)` will set the database used by Andromeda to use 10 threads.
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
copyAndromeda <- function(andromeda, options = list()) {
  checkIfValid(andromeda)
  # Call flush (checkpoint) to avoid segfault:
  Andromeda::flushAndromeda(andromeda)
  
  newAndromeda <- .createAndromeda(options = options)
  
  tables <- DBI::dbListTables(andromeda)

  if (.Platform$OS.type == "windows") {
    # On windows, avoid attaching to a locked database file
    for (table in tables) {
      tempFile <- tempfile(tmpdir = .getAndromedaTempFolder(), fileext = ".parquet")
      DBI::dbExecute(andromeda, 
        sprintf("COPY %s TO '%s' (FORMAT 'parquet')", table, tempFile))

      DBI::dbExecute(newAndromeda, 
        sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", table, tempFile))
    }
    unlink(tempFile)
  } else {
    oldFile <- andromeda@dbname
    DBI::dbExecute(newAndromeda, sprintf("ATTACH DATABASE '%s' AS old", oldFile))
    for (table in tables) {
      DBI::dbExecute(newAndromeda, 
        sprintf("CREATE TABLE %s AS SELECT * FROM old.%s", table, table))
    }
    DBI::dbExecute(newAndromeda, "DETACH DATABASE old")
  }
  if (!dplyr::setequal(names(andromeda), names(newAndromeda))) {
    succeeded <- paste(dplyr::intersect(names(andromeda), names(newAndromeda)), collapse = ", ")
    failed <- paste(dplyr::setdiff(names(andromeda), names(newAndromeda)), collapse = ", ")
    msg <- paste("Error copying Andromeda object.\n", succeeded, "copied successfully.\n", failed, "failed to copy.\n")
    rlang::abort(msg)
  } 
  return(newAndromeda)
}

# By default .createAndromeda will create a new duckdb instance in a temp folder. However it can also use an existing duckdb file.
.createAndromeda <- function(dbdir = tempfile(tmpdir = .getAndromedaTempFolder(), fileext = ".duckdb"), options = list()) {
  andromeda <- duckdb::dbConnect(duckdb::duckdb(), dbdir = dbdir)
  class(andromeda) <- "Andromeda"
  attr(class(andromeda), "package") <- "Andromeda"
  andromeda@dbname <- andromeda@driver@dbdir
  finalizer <- function(conn_ref) {
    # Suppress R Check note about unused argument:
    missing(conn_ref)
    # Use R's scoping rules to refer the andromeda object we want to close without explicitly passing it as an argument:
    close(andromeda)
  }
  reg.finalizer(andromeda@conn_ref, finalizer, onexit = TRUE)
  DBI::dbExecute(andromeda, sprintf("PRAGMA temp_directory = '%s'", normalizePath(dbdir)))
  
  # ignore all options except 'threads' for now
  if (is.numeric(options[["threads"]])) {
    DBI::dbExecute(andromeda, paste("PRAGMA threads = ", as.integer(options[["threads"]])))
  } else {
    threads <- getOption("andromedaThreads")
    if (!is.null(threads)) {
      DBI::dbExecute(andromeda, paste("PRAGMA threads = ", as.integer(threads)))
    }
  }
  memoryLimit <- getOption("andromedaMemoryLimit")
  if (is.null(memoryLimit)) {
    # Default limit is 80%, which could cause problems when there are multiple Andromeda instances:
    memoryLimit <- .getPhysicalMemory() * 0.2
  }
  DBI::dbExecute(andromeda, sprintf("SET memory_limit = '%0.4fGB';", memoryLimit))
  DBI::dbExecute(andromeda, "PRAGMA enable_progress_bar = false;")
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

  if (duckdb::dbIsValid(object)) {
    cli::cat_line(pillar::style_subtle(paste("# Physical location: ", object@driver@dbdir)))
    cli::cat_line("")
    cli::cat_line("Tables:")
    for (name in duckdb::dbListTables(object)) {
      cli::cat_line(paste0("$",
                           name,
                           " (",
                           paste(duckdb::dbListFields(object, name), collapse = ", "),
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
      duckdb::dbRemoveTable(x, i)
    }
  } else if (inherits(value, "data.frame")) {
    .checkAvailableSpace(x)
    duckdb::dbWriteTable(conn = x, name = i, value = value, overwrite = TRUE, append = FALSE)
  } else if (inherits(value, "tbl_dbi")) {
    .checkAvailableSpace(x)
    # Call flush (checkpoint) to avoid segfault:
    Andromeda::flushAndromeda(dbplyr::remote_con(value))
    if (identical(x, dbplyr::remote_con(value))) {
      # x[[i]] and value are tables are in the same Andromeda object
      sql <- dbplyr::sql_render(value, x)
      if (duckdb::dbExistsTable(x, i)) {
        # Maybe we're copying data from a table into the same table. So write to temp
        # table first, then drop old table, and rename temp to old name:
        tempName <- paste(sample(letters, 16), collapse = "")
        sql <- sprintf("CREATE TABLE %s AS %s", tempName, sql)
        DBI::dbExecute(x, sql)
        duckdb::dbRemoveTable(x, i) 
        sql <- sprintf("ALTER TABLE %s RENAME TO %s;", tempName, i)
        DBI::dbExecute(x, sql)
      } else {
        sql <- sprintf("CREATE TABLE %s AS %s", i, sql)
        DBI::dbExecute(x, sql)
      }
    } else {
      # value is not in the same database as x[[i]] 
      if (duckdb::dbExistsTable(x, i)) {
        duckdb::dbRemoveTable(x, i)
      }
      valueSourceFile <- dbplyr::remote_con(value)@dbname
      sourceTableName <- dbplyr::remote_name(value)
      if (is.null(sourceTableName) || .Platform$OS.type == "windows") {
        # value is a lazy_query or windows which has strong file locks and can't attach
        # compute first to a temp parquet file
        tempFile <- tempfile(tmpdir = .getAndromedaTempFolder(),
                             fileext = ".parquet")
        DBI::dbExecute(
          dbplyr::remote_con(value),
          sprintf(
            "COPY (%s) TO '%s' (FORMAT 'parquet')",
            dbplyr::sql_render(value), tempFile
          )
        )

        DBI::dbExecute(
          x,
          sprintf(
            "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_parquet('%s')",
            i,
            tempFile
          )
        )
        unlink(tempFile)
      } else {
          DBI::dbExecute(x, sprintf("ATTACH '%s' as source", valueSourceFile))
          DBI::dbExecute(x, sprintf(
            "CREATE OR REPLACE TABLE
            %s AS SELECT * FROM source.%s", i,
            sourceTableName
          ))
          DBI::dbExecute(x, "DETACH source")
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
  if (duckdb::dbExistsTable(x, i)) {
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
  DBI::dbListTables(x)
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
  if (!isAndromeda(x)) {
    rlang::abort(paste(deparse(substitute(x)), "is not an Andromeda object."))
  }
  return(duckdb::dbIsValid(x))
}

#' @param con    An [`Andromeda`] object.
#' @param ...	   Included for compatibility with generic `close()` method.
#' @export
#' @rdname
#' Andromeda-class
setMethod("close", "Andromeda", function(con, ...) {
  fileName <- con@driver@dbdir
  if (duckdb::dbIsValid(con)) {
    duckdb::dbDisconnect(con, shutdown = TRUE)
  }
  if (file.exists(fileName)) {
    unlink(fileName)
  }
})

checkIfValid <- function(x) {
  if (!isValidAndromeda(x)) {
    rlang::abort("Andromeda object is no longer valid. Perhaps it was saved without maintainConnection = TRUE, or R has been restarted?")
  }
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
#' @export
isAndromedaTable <- function(tbl) {
  return(inherits(tbl, "tbl_dbi") && inherits(dbplyr::remote_con(tbl), "Andromeda"))
}

.getPhysicalMemory <- function() {
  memory <- memuse::Sys.meminfo()$totalram
  memory <- memuse::swap.unit(memory, "GB")
  return(memory@size)
}
