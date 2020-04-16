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

#' Andromeda class.
#'
#' @keywords internal
#' @export
#' @import RSQLite
setClass("Andromeda", contains = "SQLiteConnection")

#' Create an Andromeda object
#' 
#' @description
#' By default the Andromeda object is created in the systems temporary file location. You can override this by 
#' specifying a folder using \code{options(andromedaTempFolder = "c:/andromedaTemp")}, where "c:/andromedaTemp"
#' is the folder to create the Andromeda objects in.
#' 
#' @param ...   Named objects. See details for what objects are valid. If no objects are provided, an empty Andromeda
#'              is returned.
#'              
#' @details 
#' Valid objects are data frames, Andromeda tables, or any other dply table. 
#' 
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#' 
#' names(andr)
#' # [1] "cars" "iris"
#' 
#' andr$cars %>% filter(speed > 10) %>% collect()
#' # # A tibble: 41 x 2
#' # speed  dist
#' # <dbl> <dbl>
#' # 1    11    17
#' #  ...
#' 
#' close(andr)
#' 
#' @export
andromeda <- function(...) {
  arguments <- list(...)
  if (length(arguments) > 0) {
    if (is.null(names(arguments)) || any(names(arguments) == ""))
      stop("All arguments must be named")
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
#' @param andromeda The andromeda object to copy.
#'
#' @return 
#' The copied andromeda.
#' 
#' @examples
#' andr <- andromeda(cars = cars, iris = iris)
#' 
#' andr2 <- copyAndromeda(andr)
#' 
#' names(andr2)
#' # [1] "cars" "iris"
#' 
#' close(andr)
#' close(andr2)
#' 
#' @export
copyAndromeda <- function(andromeda) {
  newAndromeda <- .createAndromeda()
  RSQLite::sqliteCopyDatabase(andromeda, newAndromeda)
  return(newAndromeda)
}

.createAndromeda <- function() {
  tempFolder <- getOption("andromedaTempFolder")
  if (is.null(tempFolder)) {
    tempFolder <- tempdir() 
  } else {
    if (!file.exists(tempFolder)) {
      dir.create(tempFolder, recursive = TRUE) 
    }
  }
  andromeda <- RSQLite::dbConnect(RSQLite::SQLite(), tempfile(tmpdir = tempFolder, fileext = ".sqlite"))
  class(andromeda) <- "Andromeda"
  finalizer <- function(ptr) {
    # Suppress R Check note:
    missing(ptr)
    close(andromeda)
  }
  reg.finalizer(andromeda@ptr, finalizer, onexit = TRUE) 
  return(andromeda)
}

# show()
#' @export
#' @rdname Andromeda-class
setMethod("show", "Andromeda", function(object) {
  writeLines("# Andromeda object")
  writeLines(paste("# Physical location: ", object@dbname))
  writeLines("")
  writeLines("Tables:")
  for (name in  RSQLite::dbListTables(object)) {
    writeLines(paste0("- ", name, " (", paste(RSQLite::dbListFields(object, name), collapse = ", "), ")"))
  }
  invisible(NULL)
})

#' @export
#' @rdname Andromeda-class
setMethod("$", "Andromeda", function(x, name) {
  return(x[[name]])
  
})

#' @export
#' @rdname Andromeda-class
setMethod("$<-", "Andromeda", function(x, name, value) {
  x[[name]] <- value
  return(x)
})

#' @export
#' @rdname Andromeda-class
setMethod("[[<-", "Andromeda", function(x, i, value) {
  if (is.null(value)) {
    RSQLite::dbRemoveTable(x, i)
  } else if (inherits(value, "data.frame")) {
    RSQLite::dbWriteTable(conn = x,
                          name = i,
                          value = value,
                          overwrite = TRUE,
                          append = FALSE)
  } else if (inherits(value, "tbl_dbi")) {
    if (RSQLite::dbExistsTable(x, i)) {
      RSQLite::dbRemoveTable(x, i)
    }
    if (isTRUE(all.equal(x, dbplyr::remote_con(value)))) {
      sql <-  dbplyr::sql_render(value, x)
      sql <- sprintf("CREATE TABLE %s AS %s", i, sql)
      RSQLite::dbExecute(x, sql)
    } else {
      doBatchedAppend <- function(batch) {
        RSQLite::dbWriteTable(conn = x,
                              name = i,
                              value = batch,
                              overwrite = FALSE,
                              append = TRUE)
      }
      batchApply(value, doBatchedAppend)
    }
  } else {
    stop("Table must be a data frame or dplyr table")
  }
  x
})

#' @export
#' @rdname Andromeda-class
setMethod("[[", "Andromeda", function(x, i) {
  if (RSQLite::dbExistsTable(x, i)) {
    return(dplyr::tbl(x, i))
  } else {
    return(NULL)
  }
})

#' names
#' 
#' Show the names of the tables in an Andromeda object.
#' 
#' @param x An Andromeda object.
#' 
#' @export
setMethod("names", "Andromeda", function(x) {  
  if (RSQLite::dbIsValid(x)) {
    return(RSQLite::dbListTables(x))
  }
})

#TODO : add "names<-.Andromeda"

#' @export
#' @rdname Andromeda-class
setMethod("length", "Andromeda", function(x) {
  length(names(x))
})

#' Check whether an object is an Andromeda object
#' 
#' @param x The object to check.
#' 
#' @details 
#' Checks whether an object is an Andromeda object.
#' 
#' @return 
#' A logical value.
#' 
#' @export
isAndomeda <- function(x) {
  inherits(x, "Andromeda") 
}

#' @export
#' @rdname Andromeda-class
setMethod("close", "Andromeda", function(con, ...) {
  fileName <- con@dbname
  if (RSQLite::dbIsValid(con)) {
    RSQLite::dbDisconnect(con)
  }
  if (file.exists(fileName)) {
    unlink(fileName)
  }
})

