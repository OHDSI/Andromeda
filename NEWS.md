Andromeda 1.2.0
===============

Changes:

- Explicitly settings DuckDB's `temp_directory` to the Andromeda temp folder to avoid running out of space in undefined temp location.

- Set default memory limit to 20% instead of 80% of system memory to avoid out-of-memory errors when there are multiple Andromeda objects in memory.

- `flushAndromeda()` now also evicts the cache by default, and is called after appending or creating table from another Andromeda table.

Andromeda 1.1.1
===============

Bugfixes:

- Disabling DuckDB progress bar to avoid mysterious crashes and hangs.


Andromeda 1.1.0
===============

Changes:

- Leveraging DuckDB for faster assigns and copy operations.

- Added `flushAndromeda()` function.

- Added `andromedaThreads` option to control the maximum number of threads Andromeda is allowed to use.

Bugfixes:

- Calling `flushAndromeda()` before copying entire Andromeda (`copyAndromeda()`) or just a table (`[[]]<-` operator) to avoid segfault.

- Switch from `zip::unzip()` to `utils::unzip()` to avoid 'mtimes' errors.


Andromeda 1.0.0
===============

Changes

- Switch backend from SQLite to DuckDb for greater performance in terms of speed and disk space.

- Added `andromedaMemoryLimit` option to control the maximum amount of memory Andromeda is allowed to use (in GB).


Andromeda 0.6.7
===============

Bugfixes

- Fix `isAndromedaTable()` when table belongs to descendant of Andromeda.


Andromeda 0.6.6
===============

Bugfixes

- Fix error when calling `groupApply()` with `dbplyr` >= 2.5.0.

Andromeda 0.6.5
===============

Bugfixes

- Ensuring package passes R check even when `arrow` is not installed. Required to stay in CRAN.

Andromeda 0.6.4
===============

Bugfixes

- Fix a bug causing Andromeda saving not to respect andromeda temp folder

Andromeda 0.6.3
===============

Bugfixes

- Fix workflow

Andromeda 0.6.2
===============

Changes

- Added  `isAndromedaTable()` function.

Andromeda 0.6.1
===============

Bugfixes

- Fix compatibility with dbplyr.


Andromeda 0.6.0
===============

Changes

- Provide methods to get and set Andromeda table names using the `names` and `names<-` generic functions.
- Provide methods to get and set Andromeda table column names using the `names` and `names<-` generic functions.


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