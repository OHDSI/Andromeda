Andromeda v0.4.1
---

This update has 1 bugfix related to an issue with R CMD CHECK on one of CRAN's servers.

The maintainer has been changed from Martijn Schuemie to Adam Black.

## Test environments
* Ubuntu 16.04.6 LTS (Travis), R 4.0.3
* Windows 10, R 4.0.3
* winbuilder devel

## R CMD check results

There were no ERRORs or WARNINGs.

There was 1 NOTE related to the change of maintainer.

## Downstream dependencies

This change only removes one test that was causing a problem on Fedora. 
No downstream packages will be affected by the removal of this test.