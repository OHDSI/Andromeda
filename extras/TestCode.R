library(Andromeda)

andr <- andromeda()

andr[["cars"]] <- cars

andr$cars <- cars

andr[["cars"]]


cars %>% filter(rlang::sym("speed") > 10) %>% collect()
