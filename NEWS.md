Andromeda 0.5.0
===============

Changes

- Andromeda now supports dates and datetimes ([link](https://github.com/OHDSI/Andromeda/issues/11)).

Bug Fixes

- Fix issue when loading loading large Andromeda files from disk ([link](https://github.com/OHDSI/Andromeda/issues/21)).

Andromeda 0.4.1
===============

Changes

- Remove test causing R CMD check to fail on fedora.

Andromeda 0.4.0
===============

Changes

- Added the `createIndex`, `listIndices`, and `removeIndex` functions.

Bug fixes

- Setting SQLite temp_store_directory to Andromeda temp folder as well to prevent running out of space in default drive.

- Normalizing path before querying available disk space to avoid errors when using tilde in the path.


Andromeda 0.3.1
===============

Bug fixes

- Switching to low-level Java calls to avoid error in CRAN's Debian test environment.


Andromeda 0.3.0
===============

Changes

- Throw more informative error when user tries to use invalid Andromeda object.

- Added function `getAndromedaTempDiskSpace`. Requires rJava to be installed. (Returns `NA` if not installed.)

- Throw warning when disk space becomes low. Threshold defaults to 10GB, but can be altered using `options(warnDiskSpaceThreshold = <n>)`.

- Allow assigning query result to an Andromeda table where the query uses that Andromeda table. (e.g. `andromeda$cars <- andromeda$cars %>% filter(speed > 10)`)

- Added `progressBar` arguments to `batchApply` and `groupApply`.

Bug fixes

- Unzipping now also done in Andromeda temp folder (instead of system temp folder).

- Lower but faster compression level now also used when saving without maintaining connection.


Andromeda 0.2.0
===============

Changes

- Dropping `nrow` and `ncol` support, as this seems to cause instability and is not consistent with `dplyr`.

- Dropping `isSorted` function, as database queries are only guaranteed to stay sorted if explicitly required to (using `arrange`).

- Added `restoreDate` and `restorePosixct` functions.

Bug fixes

- Fixed typo in `isAndromeda` function name.

- Fixed incompatibility issue with `dplyr` 1.0.0 causing 'method not supported' error.

- Correctly handling differences in column order when appending.

- Fixed copying of zero-row tables from one Andromeda to another.

- Fixed issue where Andromeda object inheritance was lost on R restart.


Andromeda 0.1.3
===============

Changes

- Changes in documentation for CRAN.


Andromeda 0.1.2
===============

Changes

- Adding fast `isSorted` function.

- Changes in documentation for CRAN.


Andromeda 0.1.1
===============

Changes

- Minor edits to documentation in preparation for submission to CRAN.

- Changing compression level when saving: 10-fold reduction in compression time at the cost of 10% larger file size.

- Increasing batch size from 10,000 to 100,000 to increase speed.

- Turning of SQLIte journal to increase speed.


Andromeda 0.1.0
===============

Initial version.