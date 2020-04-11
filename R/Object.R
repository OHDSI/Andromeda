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
#' @export
Andromeda <- function() {
  andromeda <- RSQLite::dbConnect(RSQLite::SQLite(), tempfile(fileext = ".sqlite"))
  class(andromeda) <- "Andromeda"
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
"$.Andromeda" <- function(x, i){
  if (RSQLite::dbExistsTable(x, i)) {
    return(dplyr::tbl(x, i))
  } else {
    return(NULL)
  }
}

#' @export
"$<-.Andromeda" <- function(x, i, value){
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
    doBatchedAppend <- function(batch) {
      RSQLite::dbWriteTable(conn = x,
                            name = i,
                            value = batch,
                            overwrite = FALSE,
                            append = TRUE)
    }
    if (isTRUE(all.equal(x, value$src$con))) {
      # Cannot read and write to the same database at the same time. 
      # Create a copy in a temp Andromeda first
      #TODO: probably should do some CREATE TABLE AS instead. ####
      tempAndromeda <- Andromeda()
      tempAndromeda$table <- value
      on.exit(RSQLite::dbDisconnect(tempAndromeda))
      value <- tempAndromeda$table
    } 
    batchApply(value, doBatchedAppend)
  }
  x
}

# names()
#' @export
setMethod("names", "Andromeda", function(x) {  
  RSQLite::dbListTables(x)
})

#TODO : add "names<-.Andromeda"

#' @export
length.Andromeda <- function(x){
  length(names(x))
}

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
is.Andomeda <- function(x) {
  inherits(x, "Andromeda") 
}

#' @export
close.Andromeda <- function(con, ...) {
  RSQLite::dbDisconnect(con)
}

