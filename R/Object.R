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
  if (length(arguments) > 0 && (is.null(names(arguments)) || any(names(arguments) == "")))
    abort("All arguments must be named")
  
  andromeda <- .newAndromeda()
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
  newAndromeda <- .newAndromeda()
  arrow::copy_files(attr(andromeda, "path"), attr(newAndromeda, "path"))
  if(!dplyr::setequal(names(andromeda), names(newAndromeda))) {
    abort(glue::glue("copyAndromeda failed.\n 
                     names(andromeda): {paste(names(andromeda), collapse = ',')} \n
                     names(newAndromeda): {paste(names(newAndromeda), collapse = ',')}"))
  }
  return(newAndromeda)
}

.newAndromeda <- function() {
  path <- tempfile(tmpdir = .getAndromedaTempFolder())
  dir.create(path)
  andromeda <- structure(list(), 
                         class = "Andromeda", 
                         path = path,
                         env = rlang::new_environment(list(path = path)))
  
  reg.finalizer(attr(andromeda, "env"), function(env) {
    r <- unlink(env$path, recursive = TRUE) 
    if(r == 1) rlang::inform("Problem with andromeda cleanup. Possible lock on Andromeda files.")
  }, onexit = TRUE)
  # attr(class(andromeda), "package") <- "Andromeda" # Why is this necessary?
  .checkAvailableSpace(andromeda)
  return(andromeda)
}

dirs <- function(x) list.dirs(attr(x, "path"), recursive = FALSE, full.names = FALSE)

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

#' @param x  An [`Andromeda`] object.
#' @param ... Included for consistency with generic. Currently ignored.
#' @export
#' @rdname
#' Andromeda-class
print.Andromeda <- function(x, ...) {
  
  if(isValidAndromeda(x)) {
    
    cli::cat_line(pillar::style_subtle("# Andromeda object"))
    cli::cat_line(pillar::style_subtle(paste("# Physical location: ", attr(x, "path"))))
    cli::cat_line("")
    cli::cat_line("Tables:")
    for (tableName in names(x)) {
      # columns <- purrr::map_chr(object[[tableName]]$schema$fields, "name")
      columns <- paste0(names(x[[tableName]]), collapse = ", ")
      cli::cat_line(paste0("$", tableName," (", columns, ")"))
    }
  } else {
    cli::cat_line(pillar::style_subtle("# Andromeda object is no longer valid. "))
  }
  invisible(NULL)
}

#' Extract Andromeda table
#'
#' @param x An andromeda object
#' @param name A character string containing the name of an Andromeda table
#'
#' @return An andromeda table
#' @export
"[[.Andromeda" <- function(x, name) {
  checkIfValid(x)
  # if(!(name %in% names(x))) {
  #   x[[name]] <- NULL
  # }
    
  if(!(name %in% names(x))) return(NULL)
  # NextMethod()
  arrow::open_dataset(file.path(attr(x, "path"), name), format = "feather")
}

#' Number of tables in an Andromeda object
#'
#' @param x An andromeda object
#'
#' @return The number of tables in the andromeda object
#' @export
"length.Andromeda" <- function(x) {
  length(names(x))
}

#' Extract table reference from Andromeda
#'
#' @param x An andromeda object
#' @param name The name of a table in the andromeda object
#'
#' @return An andromeda table
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

#' @param x An [`Andromeda`] object.
#' @param i The name of a table in the [`Andromeda`] object.
#' @param value A dataframe, [`Andromeda`] table, or dplyr query that uses an [`Andromeda`] table.
#' @export
#' @rdname
#' Andromeda-class
"[[<-.Andromeda" <- function(x, i, value) {
  if(!is.null(value) && !inherits(value, c("data.frame", "arrow_dplyr_query", "FileSystemDataset"))) {
    abort("value must be null, a dataframe, an Andromeda table, or a dplyr query using an Andromeda table")
  }
  
  removeTableIfExists <- function(x, i) {
    if (i %in% dirs(x)) {
      # on windows sometimes there is a file lock possibly added by dplyr::collect that is removed by calling gc
      gc(verbose = FALSE, full = TRUE)
      r <- unlink(file.path(attr(x, "path"), i), recursive = TRUE)
      if (r == 1) abort(paste("Removal of Andromeda dataset", i, "failed."))
    }
  }
  
  .checkAvailableSpace(x)
  
  if (is.null(value)) {
    removeTableIfExists(x, i)
  } else if (inherits(value, "data.frame")) {
    removeTableIfExists(x, i)
    dir.create(file.path(attr(x, "path"), i))
    
    if (nrow(value) == 0) {
      # write_dataset will do nothing if given a dataframe with zero rows so use write_feather instead
      arrow::write_feather(value, file.path(attr(x, "path"), i, "part-0.feather"))
    } else {
      arrow::write_dataset(value, file.path(attr(x, "path"), i), format = "feather")
    }
    value <- arrow::open_dataset(file.path(attr(x, "path"), i), format = "feather")
    
  } else if (inherits(value, "FileSystemDataset")) {
    valueDirname <- unique(dirname(value$files))
    if(length(valueDirname) != 1) abort("Only FileSystemDatasets with one or more files in a single enclosing directory are supported by Andromeda.")
    
    if (valueDirname %in% list.dirs(attr(x, "path"), recursive = FALSE, full.names = TRUE)) {
      # No need to write the FileSystemDataset since it already exists in the correct location
      value <- arrow::open_dataset(file.path(attr(x, "path"), i), format = "feather")
    } else {
      removeTableIfExists(x, i)
      arrow::write_dataset(value, file.path(attr(x, "path"), i), format = "feather")
      value <- arrow::open_dataset(file.path(attr(x, "path"), i), format = "feather")
    }
  } else if (inherits(value, c("arrow_dplyr_query"))) {
    if (i %in% dirs(x)) {
      tempDir <- tempfile("temp")
      arrow::write_dataset(value, tempDir, format = "feather")
      removeTableIfExists(x, i)
      dir.create(file.path(attr(x, "path"), i))
      arrow::copy_files(tempDir, file.path(attr(x, "path"), i))
      unlink(tempDir, recursive = TRUE)
    } else {
      arrow::write_dataset(value, file.path(attr(x, "path"), i), format = "feather")
    }
    
    # A dplyr query that results in zero rows will not be written so we need to handle that case
    if (!dir.exists(file.path(attr(x, "path"), i))) return(`[[<-`(x, i, dplyr::collect(value)))
    
    value <- arrow::open_dataset(file.path(attr(x, "path"), i), format = "feather")
  }
  NextMethod()
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
  list.dirs(attr(x, "path"), recursive = FALSE, full.names = FALSE)
}

#' Remove an andromeda object
#' 
#' Attempts to delete an andromeda object. 
#' 
#' @param con    An [`Andromeda`] object.
#' @param ...	   Included for compatibility with generic `close()` method.
#' @param verbose Should a message be printed if the attempt to remove the andromeda directory was unsuccessful (TRUE or FALSE). .
#' @return 0 for success, 1 for failure, invisibly. If the andromeda object is already closed (file does not exist) 0 is returned.
#' @export
#' @rdname Andromeda-class
"close.Andromeda" <- function(con, ..., verbose = TRUE) { 
  if (!isAndromeda(con)) abort("First argument must be an Andromeda object.")
  path <- attr(con, "path")
  rc <- 0
  if (file.exists(path)) {
    rc <- unlink(path, recursive = TRUE)
    if (rc == 1 && verbose) message("Attempt to remove andromeda file unsuccessful.")
  }
  invisible(rc)
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
isAndromeda <- function(x) inherits(x, "Andromeda")

checkIfAndromeda <- function(x) if (!isAndromeda(x)) rlang::abort("Andromeda argument must be of type 'Andromeda'")

#' Checks whether an Andromeda object is valid
#' 
#' @param x an Andromeda object
#'
#' @return TRUE if the Andromeda object is in a valid state. FALSE otherwise.
#' @export
isValidAndromeda <- function(x) {
  checkIfAndromeda(x)
  # if (!dir.exists(attr(x, "path"))) return(FALSE) 
  
  # syncNames(x)
  # nms <- attr(x, "names") %||% character(0L)
  # dirs <- list.dirs(attr(x, "path"), recursive = FALSE, full.names = FALSE)
  # isValid <- dplyr::setequal(nms, dirs)
  # return(isValid)
  dir.exists(attr(x, "path"))
}

checkIfValid <- function(x) {
  if (!isValidAndromeda(x)) abort("Andromeda object is not valid.")
}
