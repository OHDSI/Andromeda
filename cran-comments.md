This package aims to functionally replace the 'ff' package, which effectively has become orphaned now that its main dependency 'bit' has been orphaned. This way, we can remove 'bit' dependencies in our other packages in CRAN, including 'DatabaseConnector' and 'Cyclops'.

In response to your comments:

- Added value section to batchTest.Rd
- Removed setting of options in vignette
- To our knowledge there are no other attempts to modify the global environment.

---

## Test environments
* Ubuntu 14.04.5 LTS (Travis), R 3.6.2
* Windows 7, R 3.6.1 and R 4.0.0

## R CMD check results

There were no ERRORs or WARNINGs. 

## Downstream dependencies

There are no downstream dependencies.