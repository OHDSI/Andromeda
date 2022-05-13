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
#' This class is provides the ability to work with data objects in R that are too large to fit in memory. Instead, 
#' these objects are stored on disk. This is slower than working from memory, but may be the only viable option. 
#' It is essentially a list of arrow datasets that are stored on disk in the feather format.
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
NULL

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
  arrow::copy_files(attr(Andromeda, "path"), attr(newAndromeda, "path"))
  datasets <- list.dirs(attr(newAndromeda, "path"), full.names = TRUE)
  for (d in datasets) {
    newAndromeda[[d]] <- arrow::open_dataset(d, format = "feather")
  }
  if(!dplyr::setequal(names(andromeda), names(newAndromeda))) {
    abort(glue::glue("copyAndromeda failed.\n 
                     names(andromeda): {paste(names(andromeda), collapse = ',')} \n
                     names(newAndromeda): {paste(names(newAndromeda), collapse = ',')}"))
  }
  return(newAndromeda)
}

.createAndromeda <- function() {
  path <- tempfile(tmpdir = .getAndromedaTempFolder())
  dir.create(path)
  andromeda <- list()
  class(andromeda) <- "Andromeda"
  attr(class(andromeda), "package") <- "Andromeda"
  attr(andromeda, "path") <- path
  return(andromeda)
}

.getAndromedaTempFolder <- function() {
  tempFolder <- getOption("andromedaTempFolder")
  if (is.null(tempFolder)) {
    tempFolder <- tempdir()
  } else {
    tempFolder <- path.expand(tempFolder)
    if (!file.exists(tempFolder)) dir.create(tempFolder, recursive = TRUE)
  }
  return(tempFolder)
}

#' @param object  An [`Andromeda`] object.
#' @export
#' @rdname
#' Andromeda-class
print.Andromeda <- function(object) {
  cli::cat_line(pillar::style_subtle("# Andromeda object"))
  cli::cat_line(pillar::style_subtle(paste("# Physical location: ", attr(object, "path"))))
  cli::cat_line("")
  cli::cat_line("Tables:")
  for (tableName in names(object)) {
    # columns <- purrr::map_chr(object[[tableName]]$schema$fields, "name")
    columns <- paste0(names(object[[tableName]]), collapse = ", ")
    cli::cat_line(paste0("$", tableName," (", columns, ")"))
  }
  invisible(NULL)
}

#' Extract Andromeda table
#'
#' @param x An andromeda object
#' @param name A character string containing the name of an Andromeda table
#'
#' @return
#' @export
"[[.Andromeda" <- function(x, name) {
  checkIfValid(x)
  if(!(name %in% names(x))) {
    x[[name]] <- NULL
  }
  NextMethod()
}

#' Number of tables in an Andromeda object
#'
#' @param x An andromeda object
#'
#' @return
#' @export
"length.Andromeda" <- function(x) {
  length(names(x))
}

#' Extract table reference from Andromeda
#'
#' @param x An andromeda object
#' @param name The name of a table in the andromeda object
#'
#' @return
#' @export
"$.Andromeda" <- function(x, name) {
  x[[name]]
}

#' @param x     An [`Andromeda`] object.
#' @param name  The name of a table in the [`Andromeda`] object.
#' @param value A data frame, [`Andromeda`] table.
#' @export
#' @rdname
#' Andromeda-class
"$<-.Andromeda" <- function(x, name, value) {
  x[[name]] <- value
  return(x)
}

#' @param x    An [`Andromeda`] object.
#' @param i    The name of a table in the [`Andromeda`] object.
#' @param value A data frame, [`Andromeda`] table, or other 'DBI' table.
#' @export
#' @rdname
#' Andromeda-class
"[[<-.Andromeda" <- function(x, i, value) {
  checkIfValid(x)
  if(!is.null(value) && !inherits(value, c("data.frame", "arrow_dplyr_query", "FileSystemDataset"))) {
    abort("value must be null, a dataframe, an Andromeda table, or a dplyr query using an Andromeda table")
  }
  
  if (is.null(value)) {
    if (i %in% names(x)) {
      r <- unlink(file.path(attr(x, "path"), i), recursive = TRUE)
      if (r == 1) abort(paste("Removal of Andromeda dataset", i, "failed."))
    }
  } else if(inherits(value, "data.frame") && nrow(value) == 0) {
    dir.create(file.path(attr(x, "path"), i))
    arrow::write_feather(value, file.path(attr(x, "path"), i, "part-0.feather"))
    value <- arrow::open_dataset(file.path(attr(x, "path"), i), format = "feather")
  } else if(inherits(value, c("data.frame", "arrow_dplyr_query", "FileSystemDataset"))) {
    # .checkAvailableSpace(x)
    arrow::write_dataset(value, file.path(attr(x, "path"), i), format = "feather")
    value <- arrow::open_dataset(file.path(attr(x, "path"), i), format = "feather")
  }
  return(NextMethod())
}


#' The Andromeda table names
#'
#' @param x An andromeda object
#'
#' @return A character vector with table names
#' @export
#' @rdname
#' Andromeda-class
names.Andromeda <- function(x) {
  checkIfValid(x)
  list.dirs(attr(x, "path"), full.names = FALSE, recursive = FALSE)
}

#' @param con    An [`Andromeda`] object.
#' @param ...	   Included for compatibility with generic `close()` method.
#' @export
#' @rdname Andromeda-class
"close.Andromeda" <- function(con, ...) { 
  if (!isAndromeda(con)) abort("First argument must be an Andromeda object.")
  path <- attr(con, "path")
  if (file.exists(path)) unlink(path, recursive = TRUE)
}

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

isValidAndromeda <- function(x) {
  if(!isAndromeda(x)) rlang::abort(paste(deparse(substitute(x)), "is not an Andromeda object."))
  
  # nms <- names(x) %||% character(0L)
  # dirs <- list.dirs(attr(x, "path"), recursive = FALSE)
  # isValid <- dir.exists(attr(x, "path")) && dplyr::setequal(nms, dirs)
  isValid <- dir.exists(attr(x, "path"))
  return(isValid)
}

checkIfValid <- function(x) {
  if (!isValidAndromeda(x)) abort("Andromeda object is not valid.")
}
