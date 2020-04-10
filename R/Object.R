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
  # new("Andromeda")
  andromeda <- RSQLite::dbConnect(RSQLite::SQLite(), tempfile(fileext = ".sqlite"))
  class(andromeda) <- "Andromeda"
  return(andromeda)
}

#' @export
"$.Andromeda" <- function(x, i){
  dplyr::tbl(x, i)
}

#' @export
"$<-.Andromeda" <- function(x, i, value){
  RSQLite::dbWriteTable(conn = x,
                        name = i,
                        value = value,
                        overwrite = TRUE,
                        append = FALSE)
  x
}
  
#' @export
names.Andromeda <- function(x) {
  RSQLite::dbListTables(x)
}

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