library(FeatureExtraction)
options(fftempdir = "c:/fftemp")
unlink("c:/temp/test.sqlite")

nRows <- 1e5

data <- data.frame(anInteger = as.integer(runif(nRows, 0, 1e8)),
                   aReal = runif(nRows),
                   aString = sapply(1:nRows, function(x) paste(sample(c(0:9, letters, LETTERS), 10, replace = TRUE), collapse = "")),
                   aDate = as.Date(runif(nRows, 0, 1000), origin = "2000-01-01"))


library(RSQLite)
library(ffbase)
library(dplyr)
library(data.table)
library(tibble)

system.time(
  result <- data[data$anInteger == data$anInteger[100], ]
)
# user  system elapsed 
# 0       0       0 

sqliteDb <- dbConnect(RSQLite::SQLite(), "c:/temp/test.sqlite")
system.time(
  dbWriteTable(sqliteDb, "data", data)
)
# user  system elapsed 
# 0.70    0.21    0.90

system.time(
  result <- dbGetQuery(sqliteDb, sprintf("SELECT * FROM data WHERE anInteger = %s;", data$anInteger[100]))
)
# user  system elapsed 
# 0.03    0.04    0.08 

system.time(
  ffdf <- ff::as.ffdf(data)
)
# user  system elapsed 
# 0.08    0.03    0.11 

system.time(
  result <- ffdf[ffdf$anInteger == data$anInteger[100], ]
)
# user  system elapsed 
# 0.11    0.05    0.17

system.time(
  arrowTable <- arrow::Table$create(data)
)
# user  system elapsed 
# 0.19    0.02    0.20 

system.time(
  result <- arrowTable[arrowTable$anInteger == data$anInteger[100], ]
)

system.time(
  dt <- as.data.table(data)
)
# user  system elapsed 
# 0.01    0.00    0.02 

system.time(
  result <- dt[dt$anInteger == data$anInteger[100], ]
)
# user  system elapsed 
# 0.00    0.02    0.01 

system.time(
  tibble <- as_tibble(data)
)
# user  system elapsed 
# 0       0       0 

system.time(
  result <- tibble[tibble$anInteger == data$anInteger[100], ]
)
# user  system elapsed 
# 0       0       0 

print(object.size(data), units = "auto")
# 96001704 bytes

print(object.size(sqliteDb), units = "auto")
# 1912 bytes

print(object.size(ffdf), units = "auto")
# 72016472 bytes

print(object.size(arrowTable), units = "auto")
# 416 bytes

dbDisconnect(sqliteDb)
file.info("c:/temp/test.sqlite")$size
# [1] 41410560

save.ffdf(ffdf, dir = "c:/temp/testFfdf")
files <- list.files("c:/temp/testFfdf", full.names = TRUE)
sum(file.info(files)$size)
# [1] 2.4e+07

print(object.size(dt), units = "auto")
# 96002328 bytes

print(object.size(tibble), units = "auto")
# 96002520 bytes