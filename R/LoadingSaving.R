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
#' @param andromeda   An object of class Andromeda.
#' @param fileName    The path where the object will be written.
#' @param maintainConnection  Should the connection be maintained after saving? If FALSE, the Andromeda object will be invalid after this operation, but
#'                            saving will be faster.
#' @param overwrite   If the file exists, should it be overwritten? If FALSE and the file exists, an error will be thrown.
#'
#' @export
saveAndromeda <- function(andromeda, fileName, maintainConnection = FALSE, overwrite = TRUE) {
  if (!overwrite && file.exists(fileName)) {
    stop("File ", fileName, " already exists, and overwrite = FALSE") 
  }
  if (maintainConnection) {
    # Can't zip while connected, so make copy:
    tempFileName <- tempfile(fileext = ".sqlite")
    RSQLite::sqliteCopyDatabase(andromeda, tempFileName)
    zip::zipr(fileName, tempFileName)
    unlink(tempFileName)
  } else {
    RSQLite::dbDisconnect(andromeda)
    zip::zipr(fileName, andromeda@dbname)
  }
}

#' Load Andromeda from file
#'
#' @param fileName    The path where the object is stored.
#'
#' @export
loadAndromeda <- function(fileName) {
  fileNameInZip <- zip::zip_list(fileName)
  tempDir <- tempfile()
  dir.create(tempDir)
  zip::unzip(fileName, exdir = tempDir)
  newFileName <- tempfile(fileext = ".sqlite")
  file.rename(file.path(tempDir, fileNameInZip$filename), newFileName)
  unlink(tempDir)
  andromeda <- RSQLite::dbConnect(RSQLite::SQLite(), newFileName)
  class(andromeda) <- "Andromeda"
  return(andromeda)
}
