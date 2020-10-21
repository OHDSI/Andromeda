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
    msg <- paste0("File ", fileName, " already exists, and overwrite = FALSE")
    rlang::abort(msg, class = "Andromeda")
  }
  
  if (!isValidAndromeda(andromeda)) {
    rlang::abort("andromeda object is closed or not valid.")
  }
  
  fileName <- path.expand(fileName)
  if (!dir.exists(dirname(fileName))) {
    msg <- paste("The directory", dirname(fileName), "does not exist.\n",
                 "Andromeda object cannot be saved to ", fileName, ".\n")
    rlang::abort(msg, class = "Andromeda")
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
    zip::zipr(fileName, c(attributesFileName, andromeda@dbname), compression_level = 2)
    unlink(andromeda@dbname)
    rlang::inform("Disconnected Andromeda. This data object can no longer be used", class = "Andromeda")
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
  
  andromedaTempFolder <- .getAndromedaTempFolder()
  .checkAvailableSpace()
  
  # Unzip:
  tempDir <- tempfile(tmpdir = andromedaTempFolder)
  dir.create(tempDir)
  on.exit(unlink(tempDir, recursive = TRUE))
  zip::unzip(fileName, exdir = tempDir)
  
  # Rename unzipped files:
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
  RSQLite::dbExecute(andromeda, sprintf("PRAGMA temp_store_directory = '%s'", andromedaTempFolder)) 
  class(andromeda) <- "Andromeda"
  attr(class(andromeda), "package") <- "Andromeda"
  return(andromeda)
}

.checkAvailableSpace <- function(andromeda = NULL) {
  warnDiskSpace <- getOption("warnDiskSpaceThreshold")
  if (is.null(warnDiskSpace)) {
    warnDiskSpace <- 10 * 1024 ^ 3
  }
  if (warnDiskSpace != 0) {
    if (is.null(andromeda)) {
      folder <- .getAndromedaTempFolder()
    } else {
      folder <- dirname(andromeda@dbname) 
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
      
      rlang::warn(paste(message, collapse = "\n"), class = "Andromeda", .frequency = "once", .frequency_id = folder) 
      assign("lowDiskWarnings", c(lowDiskWarnings, folder), envir = andromedaGlobalEnv)
    }
  }
}

#' Get the available disk space in Andromeda temp
#' 
#' @description 
#' Attempts to determine how much disk space is still available in the Andromeda temp folder. 
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
#' if unable to determine the amount of available disk space, for example because
#' because the user doesn't have the rights to query the available disk space.
#'
#' @examples
#' # Get the number of available gigabytes:
#' getAndromedaTempDiskSpace() / 1024^3
#' #123.456
#' 
#' @export
getAndromedaTempDiskSpace <- function(andromeda = NULL) {
  if (!is.null(andromeda) && !inherits(andromeda, "SQLiteConnection")) 
    stop("Andromeda argument must be of type 'Andromeda'.")
  
  if (is.null(andromeda)) {
    folder <- .getAndromedaTempFolder()
  } else {
    folder <- dirname(andromeda@dbname) 
  }
  
  if(!dir.exists(folder)) {
    msg <- paste("Andromeda temp folder does not exist\n",
                 "folder:", folder, "\n",
                 "Cannot determine available Andromeda disk space.")
    rlang::warn(msg, class = "Andromeda")
    return(NA)
  }
  
  if(!(.Platform$OS.type %in% c("windows", "unix"))) {
    msg <- "Operating system cannot be determined.\nCannot get available Andromeda disk space.\n"
    rlang::warn(msg, class = "Andromeda")
    return(NA)
  }
  
  if (.Platform$OS.type == "windows") {
    # https://stackoverflow.com/questions/32200879/how-to-get-disk-space-of-windows-machine-with-r
    # https://stackoverflow.com/questions/293780/free-space-in-a-cmd-shell
    disks <- system("wmic logicaldisk get size,freespace,caption", inter = T)
    disks <- read.table(textConnection(disks), 
                        skip = 1,
                        col.names =  c("disk", "freeSpace", "size"),
                        row.names = NULL)
    
    idRow <- stringr::str_detect(tolower(folder), tolower(paste0("^",disks$disk)))
    if(sum(idRow) != 1) {
      msg <- paste0("Andromeda temp disk cannot be determined.\n",
                    "folder: ", folder, "\n",
                    "disks: ", paste(disks$disk, collapse = ", "), "\n",
                    "Cannot get available Andromeda disk space.\n")
      rlang::warn(msg, class = "Andromeda")
      return(NA)
    } else {
      space <- disks[idRow,]$freeSpace
    }
  }
  
  if (.Platform$OS.type == "unix") {
    # https://stat.ethz.ch/pipermail/r-help/2007-October/142319.html
    if(length(system("which df", intern = T, ignore.stderr = T)) != 1) {
      msg <- paste0("`which df` command returned error\n",
                    "Cannot get available Andromeda disk space.\n")
      rlang::warn(msg, class = "Andromeda")
      return(NA)
    } else {
      stdout <- system(paste("df", folder), intern = T, ignore.stderr = T)
      space <- as.integer(strsplit(stdout[length(stdout)], "[ ]+")[[1]][4])
    }
  }
  
  return(space)
}

.isInstalled <- function(pkg) {
  installedVersion <- tryCatch(utils::packageVersion(pkg), 
                               error = function(e) NA)
  return(!is.na(installedVersion))
}
