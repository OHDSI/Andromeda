library(dplyr)

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

