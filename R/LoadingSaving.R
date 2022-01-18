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
#' ('duckdb'). To keep the connection open, use `maintainConnection = TRUE`. This will first
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
    abort(sprintf("File %s already exists, and overwrite = FALSE", fileName))
  }
  
  if (!isValidAndromeda(andromeda)) {
    abort("andromeda object is closed or not valid.")
  }
  
  fileName <- path.expand(fileName)
  if (!dir.exists(dirname(fileName))) {
    abort(sprintf("The directory '%s' does not exist. Andromeda object cannot be saved", dirname(fileName)))
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
    # Can't zip while connected so make a copy
    andromedaCopy <- copyAndromeda(andromeda)
    tempFileName <- andromedaCopy@driver@dbdir
    duckdb::dbDisconnect(andromedaCopy, shutdown = TRUE)
    file.exists(tempFileName)
    zip::zipr(fileName, c(attributesFileName, tempFileName), compression_level = 2)
    close(andromedaCopy)
  } else {
    duckdb::dbDisconnect(andromeda, shutdown = TRUE)
    zip::zipr(fileName, c(attributesFileName, andromeda@dbname), compression_level = 2)
    close(andromeda)
    inform("Disconnected Andromeda. This data object can no longer be used")
  }
  unlink(attributesFileName)
}

#' Load Andromeda from file
#'
#' @param fileName   The path where the object was saved using [`saveAndromeda()`].
#' @param options A list containing Andromeda options. Currently the only supported option is 'threads'.
#'    Setting `options = list(threads = 10)` will set the database used by Andromeda to use 10 threads.
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
loadAndromeda <- function(fileName, options = list()) {
  if (!file.exists(fileName)) {
    abort(sprintf("File %s does not exist", fileName))
  }
  fileNamesInZip <- utils::unzip(fileName, list = TRUE)$Name
  duckdbFilenameInZip <- fileNamesInZip[grepl(".duckdb$", fileNamesInZip)]
  rdsFilenameInZip <- fileNamesInZip[grepl(".rds$", fileNamesInZip)]
  
  andromedaTempFolder <- .getAndromedaTempFolder()
  .checkAvailableSpace()
  
  # Unzip:
  tempDir <- tempfile(tmpdir = andromedaTempFolder)
  dir.create(tempDir)
  on.exit(unlink(tempDir, recursive = TRUE))
  zip::unzip(fileName, exdir = tempDir)
  
  # Rename unzipped files:
  newFileName <- tempfile(tmpdir = andromedaTempFolder, fileext = ".duckdb")
  file.rename(file.path(tempDir, duckdbFilenameInZip), newFileName)
  attributes <- readRDS(file.path(tempDir, rdsFilenameInZip))
  
  andromeda <- .createAndromeda(dbdir = newFileName, options = options)
  
  # Restore user defined attributes that don't overwrite Andromeda's reserved attributes
  for (name in dplyr::setdiff(names(attributes), names(attributes(andromeda)))) {
    attr(andromeda, name) <- attributes[[name]]
  }
  return(andromeda)
}

.checkAvailableSpace <- function(andromeda = NULL) {
  if (.isInstalled("rJava")) {
    warnDiskSpace <- getOption("warnDiskSpaceThreshold")
    if (is.null(warnDiskSpace)) {
      warnDiskSpace <- 10 * 1024 ^ 3
    }
    if (warnDiskSpace != 0) {
      if (is.null(andromeda)) {
        folder <- .getAndromedaTempFolder()
      } else {
        folder <- dirname(andromeda@driver@dbdir) 
      }
      if (exists("lowDiskWarnings", envir = andromedaGlobalEnv)) {
        lowDiskWarnings <- get("lowDiskWarnings", envir = andromedaGlobalEnv)
        if (folder %in% lowDiskWarnings) {
          # Already warned about this location. Not warning again.
          return()
        }
      } else {
        lowDiskWarnings <- c()
      }
      space <- getAndromedaTempDiskSpace(andromeda)
      if (!is.na(space) && space < warnDiskSpace) {
        message <- sprintf("Low disk space in '%s'. Only %0.1f GB left.", 
                           folder, 
                           space / 1024^3)
        
        message <- c(message, 
                     pillar::style_subtle("Use options(warnDiskSpaceThreshold = <n>) to set the number of bytes for this warning to trigger."))
        message <- c(message, 
                     pillar::style_subtle("This warning will not be shown for this file location again during this R session."))
        
        warn(paste(message, collapse = "\n")) 
        assign("lowDiskWarnings", c(lowDiskWarnings, folder), envir = andromedaGlobalEnv)
      }
    }
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
#' The number of bytes of available disk space in the Andromeda temp folder. Returns NA
#' if unable to determine the amount of available disk space, for example because `rJava` 
#' is not installed, or because the user doesn't have the rights to query the available 
#' disk space.
#'
#' @examples
#' # Get the number of available gigabytes:
#' getAndromedaTempDiskSpace() / 1024^3
#' #123.456
#' 
#' @export
getAndromedaTempDiskSpace <- function(andromeda = NULL) { 
  if (!is.null(andromeda) && !inherits(andromeda, "duckdb_connection")) 
    abort("Andromeda argument must be of type 'Andromeda'.")
  
  # Using Java because no cross-platform functions available in R:
  if (!.isInstalled("rJava")) {
    return(NA)
  } else {
    if (is.null(andromeda)) {
      folder <- .getAndromedaTempFolder()
    } else {
      folder <- dirname(andromeda@driver@dbdir) 
    }
    space <- tryCatch({
      rJava::.jinit()
      file <- rJava::.jnew("java.io.File", normalizePath(folder), check = FALSE, silent = TRUE)
      rJava::.jcall(file, "J", "getUsableSpace")
      
      # This throws "illegal reflective access operation" warning:
      # path <- rJava::J("java.nio.file.Paths")$get(fileName, rJava::.jarray(c("")))
      # fileStore <- rJava::J("java.nio.file.Files")$getFileStore(path)
      # fileStore$getUsableSpace()
    }, error = function(e) NA)
    return(space)
  }
}

.isInstalled <- function(pkg) {
  installedVersion <- tryCatch(utils::packageVersion(pkg), 
                               error = function(e) NA)
  return(!is.na(installedVersion))
}

# get the user defined attributes of an andromeda object as a named list
.userDefinedAttributes <- function(andromeda) {
  attribs <- attributes(andromeda)
  for (name in slotNames(andromeda)) {
    attribs[[name]] <- NULL
  }
  attribs[["class"]] <- NULL
  attribs
}
