# library(testthat)
# 
# test_that("Append from same andromeda", {
#   andromeda <- andromeda()
#   andromeda$cars <- cars
#   appendToTable(andromeda$cars, andromeda$cars %>% filter(speed > 10))
# 
#   carsPlus2 <- andromeda$cars %>% collect()
# 
#   carsPlus <- rbind(cars, cars[cars$speed > 10, ])
#   expect_equivalent(carsPlus2, carsPlus)
#   close(andromeda)
# })
# 
# test_that("Append from other andromeda", {
#   andromeda <- andromeda()
#   andromeda$cars <- cars
# 
#   andromeda2 <- andromeda()
#   andromeda2$cars <- cars
#   appendToTable(andromeda$cars, andromeda2$cars %>% filter(speed > 10))
# 
#   carsPlus2 <- andromeda$cars %>% collect()
# 
#   carsPlus <- rbind(cars, cars[cars$speed > 10, ])
#   expect_equivalent(carsPlus2, carsPlus)
#   close(andromeda)
#   close(andromeda2)
# })
# 
# test_that("Append from data frame", {
#   andromeda <- andromeda()
#   andromeda$cars <- cars
# 
#   appendToTable(andromeda$cars, cars[cars$speed > 10, ])
# 
#   carsPlus2 <- andromeda$cars %>% collect()
# 
#   carsPlus <- rbind(cars, cars[cars$speed > 10, ])
#   expect_equivalent(carsPlus2, carsPlus)
#   close(andromeda)
# })
# 
# test_that("Append from same andromeda with switched column order", {
#   andromeda <- andromeda()
#   andromeda$cars <- cars
#   appendToTable(andromeda$cars, andromeda$cars %>% filter(speed > 10) %>% select(dist, speed))
#   
#   carsPlus2 <- andromeda$cars %>% collect()
#   
#   carsPlus <- rbind(cars, cars[cars$speed > 10, ])
#   expect_equivalent(carsPlus2, carsPlus)
#   close(andromeda)
# })
# 
# test_that("Append from other andromeda with switched column order", {
#   andromeda <- andromeda()
#   andromeda$cars <- cars
#   
#   andromeda2 <- andromeda()
#   andromeda2$cars <- cars
#   appendToTable(andromeda$cars, andromeda2$cars %>% filter(speed > 10) %>% select(dist, speed))
#   
#   carsPlus2 <- andromeda$cars %>% collect()
#   
#   carsPlus <- rbind(cars, cars[cars$speed > 10, ])
#   expect_equivalent(carsPlus2, carsPlus)
#   close(andromeda)
#   close(andromeda2)
# })
# 
# test_that("Append from data frame with switched column order", {
#   andromeda <- andromeda()
#   andromeda$cars <- cars
#   
#   appendToTable(andromeda$cars, cars[cars$speed > 10, c("dist", "speed")])
#   
#   carsPlus2 <- andromeda$cars %>% collect()
#   
#   carsPlus <- rbind(cars, cars[cars$speed > 10, ])
#   expect_equivalent(carsPlus2, carsPlus)
#   close(andromeda)
# })
# 
