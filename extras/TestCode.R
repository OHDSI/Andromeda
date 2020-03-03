# Create an empty object:
andromeda <- Andromeda()

# Add some data:
DBI::dbWriteTable(andromeda, "cars", cars)

# Data is there:
DBI::dbListTables(andromeda)

# Save to disk:
saveAndromeda(andromeda, "c:/temp/test.andromeda", maintainConnection = TRUE)

# We can still use the object, because maintainConnection = TRUE. Note that changes to the object have no effect on saved object.
DBI::dbListTables(andromeda)

# This is what happens if you have maintainConnection = FALSE:
saveAndromeda(andromeda, "c:/temp/test.andromeda")
DBI::dbListTables(andromeda) # Error

# We can load the object at a later point in time:
andromeda2 <- loadAndromeda("c:/temp/test.andromeda")
DBI::dbListTables(andromeda2)

