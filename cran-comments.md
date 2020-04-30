This package aims to functionally replace the 'ff' package, which effectively has become orphaned now that its main dependency 'bit' has been orphaned. This way, we can remove 'bit' dependencies in our other packages in CRAN, including 'DatabaseConnector' and 'Cyclops'.

In response to your comments:

- Added 'Observational Health Data Science and Informatics' as copyright holder to DESCRIPTION (‘cph’ role in the ‘Authors@R’).
- Changed package description so it does not start with the package name.
- Package names are no longer in the description, so no single quotes are required.
- No literature references are applicable to cite at this point in time.
- All functions now have a \value section in their documentation.
- loadAndromeda now uses a file that is created in the example itself in a temp location, and deleted afterwards (and no longer uses \dontrun{})
- The saveAndromeda example no longer writes to home filespace (and no longer uses \dontrun{})

---

## Test environments
* Ubuntu 14.04.5 LTS (Travis), R 3.6.2
* Windows 7, R 3.6.1 and R 4.0.0

## R CMD check results

There were no ERRORs or WARNINGs. 

## Downstream dependencies

There are no downstream dependencies.