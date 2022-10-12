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
#' @param overwrite            If the file exists, should it be overwritten? If `FALSE` and the file
#'                             exists, an error will be thrown.
#'
#' @seealso
#' \code{\link{loadAndromeda}}
#'
#' @description
#' Saves the [`Andromeda`] object in a zipped file.
#' Each dataset is represented as a folder contiaining one or more feather files.
#' User defined attributes are saved to a json file.
#'
#' @return 
#' Invisibly returns the andromeda object being saved.
#'
#' @examples
#' \dontrun{
#' andr <- andromeda(cars = cars)
#' 
#' # For this example we'll use a temporary file location:
#' fileName <- tempfile()
#' 
#' saveAndromeda(andr, fileName)
#' 
#' # Cleaning up the file used in this example:
#' unlink(fileName)
#' }
#' @seealso 
#' [`loadAndromeda()`]
#'
#' @export
saveAndromeda <- function(andromeda, fileName, overwrite = TRUE) {
  checkIfValid(andromeda)
  if (!overwrite && file.exists(fileName)) {
    abort(sprintf("File %s already exists, and overwrite = FALSE", fileName))
  }
  
  fileName <- path.expand(fileName)
  if (!dir.exists(dirname(fileName))) {
    abort(sprintf("The directory '%s' does not exist. Andromeda object cannot be saved", dirname(fileName)))
  }  
  
  # Need to save any user-defined attributes as well:
  attribs <- attributes(andromeda)
  attribs[["class"]] <- attribs[["path"]] <- attribs[["names"]] <- attribs[["env"]] <- attribs[[".xData"]] <- NULL
  attributesFileName <- file.path(tempdir(), "user-defined-attributes.json")
  jsonlite::write_json(attribs, attributesFileName)
  
  tableDirs <- list.dirs(attr(andromeda, "path"), recursive = FALSE)
  zip::zipr(fileName, c(attributesFileName, tableDirs), compression_level = 2)
  unlink(attributesFileName)
  invisible(andromeda)
}

#' Load Andromeda from file
#'
#' @param fileName The path where the object was saved using [`saveAndromeda()`].
#'
#' @seealso
#' [`saveAndromeda()`]
#' 
#' @return 
#' An [`Andromeda`] object.
#'
#' @examples
#' \dontrun{
#' # For this example we create an Andromeda object and save it to
#' # a temporary file location
#' fileName <- tempfile()
#' andr <- andromeda(cars = cars)
#' saveAndromeda(andr, fileName)
#' 
#' # Using loadAndromeda to load the object back:
#' andr <- loadAndromeda(fileName)
#' 
#' 
#' # Cleaning up the file used in this example:
#' unlink(fileName)
#' }
#' @export
loadAndromeda <- function(fileName) {
  if (!file.exists(fileName)) abort(sprintf("File %s does not exist", fileName))
  
  fileNamesInZip <- utils::unzip(fileName, list = TRUE)$Name
  
  andromeda <- .newAndromeda()
  path <- andromeda@path
  zip::unzip(fileName, exdir = path)
  tableNames <- list.dirs(path, full.names = FALSE, recursive = FALSE)
  
  for (nm in tableNames) {
    andromeda[[nm]] <- arrow::open_dataset(file.path(path, nm), format = "feather")
  }
  
  attributes <- jsonlite::read_json(file.path(path, "user-defined-attributes.json"), simplifyVector = TRUE)
  on.exit(unlink(file.path(path, "user-defined-attributes.json")))
  for (nm in names(attributes)) {
    attr(andromeda, nm) <- attributes[[nm]]
  }
  return(andromeda)
}

.checkAvailableSpace <- function(andromeda = NULL) {
  warnDiskSpace <- getOption("warnDiskSpaceThreshold") %||% 10
  if (!.isInstalled("rJava") || warnDiskSpace <= 0) return()
  space <- getAndromedaTempDiskSpace(andromeda)
  folder <- attr(andromeda, "path") %||% .getAndromedaTempFolder()
  
  if (!is.na(space) && space < warnDiskSpace) {
    line1 <- sprintf("Low disk space in '%s'. Only %0.1f GB left.", folder, space)
    line2 <- "Use options(warnDiskSpaceThreshold = <n>) to set the number of gigabytes for this warning to trigger."
    msg <- paste(line1,"\n", pillar::style_subtle(line2))
    warn(msg, .frequency = "once", .frequency_id = folder)
  }
}

#' Get the available disk space in Andromeda temp
#' 
#' @description 
#' Attempts to determine how much disk space is still available in the Andromeda temp folder. 
#' This function uses Java, so will only work if the `rJava` package is installed.
#' 
#' By default the Andromeda temp folder is located in the system temp space, but the location
#' can be altered using `options(andromedaTempFolder = "c:/andromedaTemp")`, where
#' `"c:/andromedaTemp"` is the folder to create the Andromeda objects in.
#' 
#' @param andromeda  Optional: provide an [Andromeda] object for which to get the available disk 
#'                   space. Normally all [Andromeda] objects use the same temp folder, but the user
#'                   could have altered it. 
#'
#' @return
#' The number of gigabytes of available disk space in the Andromeda temp folder. Returns NA
#' if unable to determine the amount of available disk space, for example because `rJava` 
#' is not installed, or because the user doesn't have the rights to query the available 
#' disk space.
#'
#' @examples
#' \dontrun{
#' # Get the number of available gigabytes:
#' getAndromedaTempDiskSpace()
#' #123.456
#' }
#' @export
getAndromedaTempDiskSpace <- function(andromeda = NULL) {
  if(!is.null(andromeda)) checkIfValid(andromeda)
     
  # Using Java because no cross-platform functions available in R:
  if (!.isInstalled("rJava")) return(NA)
  folder <- attr(andromeda, "path") %||% .getAndromedaTempFolder()
  space <- tryCatch({
    rJava::.jinit()
    file <- rJava::.jnew("java.io.File", normalizePath(folder), check = FALSE, silent = TRUE)
    rJava::.jcall(file, "J", "getUsableSpace")
    
    # This throws "illegal reflective access operation" warning:
    # path <- rJava::J("java.nio.file.Paths")$get(fileName, rJava::.jarray(c("")))
    # fileStore <- rJava::J("java.nio.file.Files")$getFileStore(path)
    # fileStore$getUsableSpace()
  }, error = function(e) NA)
  space <- round(space/(1000^3), digits = 2)
  return(space)
}

#' Check that Andromeda temp folder 
#' 
#' Checks that the Andromeda temp folder exists, is writable, and optionally has a minimum required amount of available disk space.
#' The Andromeda temp folder is the location where Andromeda objects are stored. 
#'
#' @param minimumSize (Optional) The minimum required number of available gigabytes for the Andromeda temp folder.
#'
#' @return This function is not called for its return value. 
#' @export
#'
#' @examples
#' \dontrun{
#' # Check that the Andromeda temp folder has at least 10GB. 
#' checkAndromedaTempFolder(minimumSize = 10)
#' }
checkAndromedaTempFolder <- function(minimumSize) {
  folder <- .getAndromedaTempFolder()
  if(!file.exists(folder)) 
    rlang::abort(paste("Andromeda temp folder", folder, "does not exist.\nSet option andromedaTempFolder to a writatble file location."))
  
  tryCatch({a <- andromeda(df = data.frame(test = "test"))},
           error = function(e) rlang::abort("Andromeda temp folder is not writable."),
           finally = close(a))
  
  a <- andromeda(df = data.frame(test = "test"))
  on.exit(close(a), add = TRUE)
  if (!missing(minimumSize)) {
    if (!.isInstalled("rJava")) rlang::abort("rJava is required to check minimum size of Andromeda temp folder but is not installed.")
    tempSpace <- getAndromedaTempDiskSpace(a)
    if (is.na(tempSpace)) rlang::abort("Error getting temp disk space with `getAndromedaTempDiskSpace`")
    if (tempSpace <  minimumSize) {
      rlang::abort(paste("Andromeda temp location has less available space than minimum required size.", 
                         "\navailable space:", getAndromedaTempDiskSpace(a), "gigabytes", 
                         "\nminimumSize:", minimumSize, "gigabytes\n"))
    }
  }
}

.isInstalled <- function(pkg) {
  installedVersion <- tryCatch(utils::packageVersion(pkg), error = function(e) NA)
  return(!is.na(installedVersion))
}
