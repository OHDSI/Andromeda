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

#' Save Andromeda to file
#'
#' @param andromeda            An object of class [`Andromeda`].
#' @param fileName             The path where the object will be written.
#' @param maintainConnection   Should the connection be maintained after saving? If `FALSE`, the
#'                             Andromeda object will be invalid after this operation, but saving will
#'                             be faster.
#' @param overwrite            If the file exists, should it be overwritten? If `FALSE` and the file
#'                             exists, an error will be thrown.
#'
#' @seealso
#' \code{\link{loadAndromeda}}
#'
#' @description
#' Saves the [`Andromeda`] object in a zipped file. Note that by default the [`Andromeda`] object is
#' automatically closed by saving it to disk. This is due to a limitation of the underlying technology
#' ('RSQLite'). To keep the connection open, use `maintainConnection = TRUE`. This will first
#' create a temporary copy of the [`Andromeda`] object. Note that this can be substantially slower.
#'
#' @return 
#' Returns no value. Executed for the side-effect of saving the object to disk.
#'
#' @examples
#' andr <- andromeda(cars = cars)
#' 
#' # For this example we'll use a temporary file location:
#' fileName <- tempfile()
#' 
#' saveAndromeda(andr, fileName)
#' 
#' # Cleaning up the file used in this example:
#' unlink(fileName)
#' 
#' @seealso 
#' [`loadAndromeda()`]
#'
#' @export
saveAndromeda <- function(andromeda, fileName, maintainConnection = FALSE, overwrite = TRUE) {
  if (!overwrite && file.exists(fileName)) {
    stop("File ", fileName, " already exists, and overwrite = FALSE")
  }
  # Need to save any user-defined attributes as well:
  attribs <- attributes(andromeda)
  for (name in slotNames(andromeda)) {
    attribs[[name]] <- NULL
  }
  attribs[["class"]] <- NULL

  attributesFileName <- tempfile(fileext = ".rds")
  saveRDS(attribs, attributesFileName)

  if (maintainConnection) {
    # Can't zip while connected, so make copy:
    tempFileName <- tempfile(fileext = ".sqlite")
    RSQLite::sqliteCopyDatabase(andromeda, tempFileName)
    zip::zipr(fileName, c(attributesFileName, tempFileName), compression_level = 2)
    unlink(tempFileName)
  } else {
    RSQLite::dbDisconnect(andromeda)
    zip::zipr(fileName, c(attributesFileName, andromeda@dbname))
    unlink(andromeda@dbname)
    writeLines("Disconnected Andromeda. This data object can no longer be used")
  }
  unlink(attributesFileName)
}

#' Load Andromeda from file
#'
#' @param fileName   The path where the object was saved using [`saveAndromeda()`].
#'
#' @seealso
#' [`saveAndromeda()`]
#' 
#' @return 
#' An [`Andromeda`] object.
#'
#' @examples
#' # For this example we create an Andromeda object and save it to
#' # a temporary file locationL
#' fileName <- tempfile()
#' andr <- andromeda(cars = cars)
#' saveAndromeda(andr, fileName)
#' 
#' # Using loadAndromeda to load the object back:
#' andr <- loadAndromeda(fileName)
#' 
#' # Don't forget to close Andromeda when you are done:
#' close(andr)
#' 
#' # Cleaning up the file used in this example:
#' unlink(fileName)
#'
#' @export
loadAndromeda <- function(fileName) {
  fileNamesInZip <- zip::zip_list(fileName)$filename
  sqliteFilenameInZip <- fileNamesInZip[grepl(".sqlite$", fileNamesInZip)]
  rdsFilenameInZip <- fileNamesInZip[grepl(".rds$", fileNamesInZip)]
  tempDir <- tempfile()
  dir.create(tempDir)
  on.exit(unlink(tempDir, recursive = TRUE))
  zip::unzip(fileName, exdir = tempDir)

  andromedaTempFolder <- .getAndromedaTempFolder()
  newFileName <- tempfile(tmpdir = andromedaTempFolder, fileext = ".sqlite")
  file.rename(file.path(tempDir, sqliteFilenameInZip), newFileName)
  attributes <- readRDS(file.path(tempDir, rdsFilenameInZip))
  andromeda <- RSQLite::dbConnect(RSQLite::SQLite(), newFileName)
  finalizer <- function(ptr) {
    # Suppress R Check note:
    missing(ptr)
    close(andromeda)
  }
  reg.finalizer(andromeda@ptr, finalizer, onexit = TRUE)
  for (name in names(attributes)) {
    attr(andromeda, name) <- attributes[[name]]
  }
  RSQLite::dbExecute(andromeda, "PRAGMA journal_mode = OFF") 
  class(andromeda) <- "Andromeda"
  return(andromeda)
}
