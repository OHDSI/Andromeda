% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Operations.R
\name{restoreDate}
\alias{restoreDate}
\title{Restore dates}
\usage{
restoreDate(x)
}
\arguments{
\item{x}{A numeric vector representing dates.}
}
\value{
A vector of type \code{Date}.
}
\description{
This function has been deprecated since Andromeda v0.5 preserves dates.
}
\examples{
myData <- data.frame(startDate = as.Date(c("2000-01-01", "2001-01-31", "2004-12-31")))
andr <- andromeda(myData = myData)

andr$myData \%>\% 
  collect() \%>\%
  mutate(startDate = restoreDate(startDate))
# # A tibble: 3 x 1
# startDate 
# <date>    
# 1 2000-01-01
# 2 2001-01-31
# 3 2004-12-31

close(andr)

}
\seealso{
\code{\link[=restorePosixct]{restorePosixct()}}
}
