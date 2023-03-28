Andromeda
=========

[![Build Status](https://github.com/OHDSI/Andromeda/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/Andromeda/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/Andromeda/coverage.svg?branch=main)](https://app.codecov.io/gh/OHDSI/Andromeda?branch=main)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/Andromeda)](https://cran.r-project.org/package=Andromeda)
[![CRAN_Status_Badge](http://cranlogs.r-pkg.org/badges/Andromeda)](https://cran.r-project.org/package=Andromeda)

Andromeda is part of [HADES](https://ohdsi.github.io/Hades/).

Introduction
============
AsynchroNous Disk-based Representation of MassivE DAta (ANDROMEDA): An R package for storing large data objects. Andromeda allow storing data objects on a local drive, while still making it possible to manipulate the data in an efficient manner.  

Features
========
- Allows storage of data objects much larger than what can fit in memory.
- Integrates with [dplyr package](https://dplyr.tidyverse.org/) for data manipulation.
- Objects are stored in a temporary location on the local file system.
- Ability to save and load the objects to a compressed file in a permanent location on the local file system.

Examples
========
```r
library(Andromeda)
bigData <- andromeda()

# Add some 'big' data:
bigData$cars <- cars

# Manipulate using dplyr:
bigData$cars %>% filter(speed > 10) %>% count() %>% collect()
# # A tibble: 1 x 1
#       n
#   <int>
# 1    41

saveAndromeda(bigData, "bigData.zip")
close(bigData)
```

Technology
==========
The Andromeda package is an R package wrapped around RSQLite.

System Requirements
===================
Running the package requires R.

Installation
=============
To install the latest development version, install from GitHub:

```r
install.packages("devtools")
devtools::install_github("ohdsi/Andromeda")
```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/Andromeda/).

* Vignette: [Using Andromeda](https://ohdsi.github.io/Andromeda/articles/UsingAndromeda.html)
* Package manual: [Andromeda manual](https://ohdsi.github.io/Andromeda/reference/index.html) 

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/Andromeda/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
Andromeda is licensed under Apache License 2.0

Development
===========
Andromeda is being developed in R Studio.

### Development status

Beta. Use at your own risk.

