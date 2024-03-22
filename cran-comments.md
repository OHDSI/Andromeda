Andromeda v0.6.6
---

This is a patch release that fixes the error observed on various CRAN testing instances following the release of dbplyr 2.5.0.

## Test environments
* Windows-latest, R-release (Github Actions)
* macOS-latest, R-release (Github Actions)
* Ubuntu-20.04, R-release (Github Actions)
* Ubuntu-20.04, R-devel (Github Actions)
* Winbuilder, R-release


## R CMD check results

There were no ERRORs or WARNINGs on any platform.

## Downstream dependencies

DatabaseConnector is used by Cyclops, TreatmentPatterns, CohortAlgebra, DatabaseConnector, which were tested with this new version. No issues were found.
