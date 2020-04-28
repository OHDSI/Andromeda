This package aims to functionally replace the 'ff' package, which effectively has become orphaned now that its main dependency 'bit' has been orphaned. This way, we can remove 'bit' dependencies in our other packages in CRAN, including 'DatabaseConnector' and 'Cyclops'.

In response to your comments:

- Added 'Observational Health Data Science and Informatics' as copyright holder to README
- Changed description so it does not start with the package name.
- Package names are no longer in the description, so no single quotes are required.
- No references are applicable at this point in time.
- 

---

## Test environments
* Ubuntu 14.04.5 LTS (Travis), R 3.6.2
* Windows 7, R 3.6.1 and R 4.0.0

## R CMD check results

There were no ERRORs or WARNINGs. 

## Downstream dependencies

There are no downstream dependencies.