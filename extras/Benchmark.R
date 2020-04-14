library(ffbase)
library(dplyr)
library(Andromeda)
options(fftempdir = "s:/fftemp")
# Needed to prevent "NAs introduced by coercion to integer range" in ff:
library(FeatureExtraction)


# Using random data -------------------------------------

nRows <- 1e6
data <- data.frame(anInteger = as.integer(runif(nRows, 0, 1e8)),
                   aReal = runif(nRows),
                   aString = sapply(1:nRows, function(x) paste(sample(c(0:9, letters, LETTERS), 10, replace = TRUE), collapse = "")),
                   aDate = as.Date(runif(nRows, 0, 1000), origin = "2000-01-01"))


#### Storage  ####
system.time({
  andromeda <- andromeda()
  andromeda$data <- data
})
# user  system elapsed 
# 1.00    0.06    1.06 

system.time(
  ffdf <- ff::as.ffdf(data)
)
# user  system elapsed 
# 0.11    0.03    0.14 

#### Selection ####
system.time(
  result <- data[data$anInteger == data$anInteger[100], ]
)
# user  system elapsed 
# 0       0       0 

system.time(
  result <- ffdf[ffdf$anInteger == data$anInteger[100], ]
)
# user  system elapsed 
# 0.05    0.03    0.09 

system.time({
  value <- data$anInteger[100]
  andromeda$result <- andromeda$data %>% filter(anInteger == value)
})
# user  system elapsed 
#  0.07    0.04    0.11

#### Saving ####
system.time({
  saveRDS(data, "c:/temp/testDataFrame.rds")
})
# user  system elapsed 
# 4.49    0.05    4.81 

system.time({
  save.ffdf(ffdf, dir = "c:/temp/testFfdf")
})
# user  system elapsed 
# 1.95    0.01    2.47   

system.time({
  saveAndromeda(andromeda, "c:/temp/testAndromeda.zip")
})
# user  system elapsed 
# 8.41    0.06    8.52 

#### Object size in memory ####
print(object.size(data), units = "auto")
# 91.6 Mb

print(object.size(ffdf), units = "auto")
# 68.7 Mb

print(object.size(andromeda), units = "auto")
# 1.7 Kb

#### Object size stored on disk ####
sprintf("%0.1f MB", file.size("c:/temp/testDataFrame.rds") / 1024^2)
# [1] "32.0 MB"

files <- list.files("c:/temp/testFfdf", full.names = TRUE)
sprintf("%0.1f MB", sum(file.size(files)) / 1024^2)
# [1] "22.9 MB"

sprintf("%0.1f MB", file.size("c:/temp/testAndromeda.zip") / 1024^2)
# [1] "29.7 MB"

# Using data from database ------------------------------------------------------
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"))
connection <- connect(connectionDetails)
cdmDatabaseSchema <- "cdm_synpuf_v667.dbo"

sql <- "SELECT * FROM @cdm_database_schema.drug_era;"

#### Downloading ####

system.time({
  andromeda <- andromeda()
  renderTranslateQuerySqlToAndromeda(connection, sql, andromeda = andromeda, andromedaTableName = "data", cdm_database_schema = cdmDatabaseSchema, snakeCaseToCamelCase = TRUE)
})
# user  system elapsed 
# 8309.19   36.42 3583.80 

system.time({
  data <- andromeda$data %>% collect()
})
# user  system elapsed 
# 126.78   10.74  137.53 

system.time({
  ffdf <- ff::as.ffdf(data)
})
# user  system elapsed 
# 3.62    6.81   10.47

#### Selection ####
system.time(
  result <- data[data$drugConceptId == 1124300, ]
)
# user  system elapsed 
# 0.89    0.20    1.09 

system.time(
  result <- ffdf[ffdf$drugConceptId == 1124300, ]
)
# user  system elapsed 
# 2.90    1.82    4.78 

system.time({
  andromeda$result <- andromeda$data %>% filter(drugConceptId == 1124300)
})
# user  system elapsed 
# 6.94    5.60   12.59 

#### Saving ####
system.time({
  saveRDS(data, "c:/temp/testDbDataFrame.rds")
})
# user  system elapsed 
# 445.05    1.94  447.61

system.time({
  save.ffdf(ffdf, dir = "c:/temp/testDbFfdf")
})
# user  system elapsed 
# 0.16    8.64  196.84 

system.time({
  saveAndromeda(andromeda, "c:/temp/testDbAndromeda.zip")
})
# user  system elapsed 
# 720.94    3.77  731.48 

#### Loading ####

system.time({
  andromeda <- loadAndromeda("c:/temp/testDbAndromeda.zip")
})
# user  system elapsed 
# 23.18    3.09   28.59 

#### Object size in memory ####
print(object.size(data), units = "auto")
# 6.5 Gb

print(object.size(ffdf), units = "auto")
# 25.1 Kb

print(object.size(andromeda), units = "auto")
# 1.7 Kb

#### Object size stored on disk ####
sprintf("%0.1f MB", file.size("c:/temp/testDbDataFrame.rds") / 1024^2)
# [1] "1515.0 MB"

files <- list.files("c:/temp/testDbFfdf", full.names = TRUE)
sprintf("%0.1f MB", sum(file.size(files)) / 1024^2)
# [1] "6677.2 MB"

sprintf("%0.1f MB", file.size("c:/temp/testDbAndromeda.zip") / 1024^2)
# [1] "2010.5 MB"
