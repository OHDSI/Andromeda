library(Andromeda)

andr <- andromeda()

andr[["cars"]] <- cars

andr$cars <- cars

andr[["cars"]]


cars %>% filter(speed > 10) %>% collect()
