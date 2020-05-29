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

#' @keywords internal
#' @aliases
#' NULL Andromeda-package
#' @importFrom methods slotNames
#' @importFrom utils head setTxtProgressBar txtProgressBar
#' @importFrom tidyselect all_of
#' @import dplyr
#' 
"_PACKAGE"

# Used to remember when a warning has already been thrown for a temp file location:
andromedaGlobalEnv <- new.env(parent = emptyenv())
