library(dplyr)
library(Andromeda)

# Create an empty object:
andromeda <- Andromeda()

is.Andomeda(andromeda)

# folder <- dirname(andromeda@dbname)
# list.files(folder)

# Add some data:
andromeda$cars <- cars

names(andromeda)
andromeda$cars %>% collect()

andromeda$cars %>% count() %>% collect()

attr(andromeda, "metaData") <- list(x = 1)

# Save to disk:
saveAndromeda(andromeda, "c:/temp/test.andromeda", maintainConnection = TRUE)

# We can still use the object, because maintainConnection = TRUE. Note that changes to the object have no effect on saved object.
names(andromeda)

# This is what happens if you have maintainConnection = FALSE:
saveAndromeda(andromeda, "c:/temp/test.andromeda")
names(andromeda) # Error

# We can load the object at a later point in time:
andromeda2 <- loadAndromeda("c:/temp/test.andromeda")
names(andromeda2)
attr(andromeda, "metaData")

# Batching ----------------------------------------
library(dplyr)
library(Andromeda)
andromeda <- Andromeda()
andromeda$cars <- cars

doSomething <- function(batch) {
  print(nrow(batch))
}
collectBatched(andromeda$cars, doSomething)

andromeda$cars2 <- andromeda$cars
andromeda$iris <- iris
andromeda$cars2 <- andromeda$iris

# Append ---------------------------------------------
andromeda$cars %>% count() %>% collect()

appendToTable(andromeda$cars, andromeda$cars2)

andromeda$cars %>% count() %>% collect()

appendToTable(andromeda$cars, cars)

andromeda$cars %>% count() %>% collect()

andromeda <- Andromeda()
andromeda$cars <- cars
andromeda2 <- Andromeda()
andromeda2$cars <- cars

andromeda$cars %>% count() %>% collect()
appendToTable(andromeda$cars, andromeda2$cars)
andromeda$cars %>% count() %>% collect()



all.equal(andromeda$cars$src$con, andromeda$cars2$src$con)



