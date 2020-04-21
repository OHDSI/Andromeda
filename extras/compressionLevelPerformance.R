computePerformance <- function(compressionLevel) {
  writeLines(sprintf("Testing compression level %s", compressionLevel))
  unlink("c:/temp/compressed.zip")
  start <- Sys.time()
  zip::zipr("c:/temp/compressed.zip", "c:/temp/compressThis.sqlite", compression_level = compressionLevel)
  delta <- as.numeric(difftime(Sys.time(), start, units = "secs"))
  return(tibble::tibble(compressionLevel = compressionLevel,
                        seconds = delta,
                        megabytes = file.size("c:/temp/compressed.zip") / 1024 ^ 2))
}

performance <- lapply(1:9, computePerformance)
performance <- bind_rows(performance)
readr::write_csv(performance, "extras/compressionLevelPerformance.csv")

performance <- readr::read_csv("extras/compressionLevelPerformance.csv")

library(ggplot2)
vizData <- bind_rows(tibble::tibble(compressionLevel = performance$compressionLevel,
                                    value = performance$seconds,
                                    metric = "Seconds"),
                     tibble::tibble(compressionLevel = performance$compressionLevel,
                                    value = performance$megabytes,
                                    metric = "Megabytes"))
ggplot(vizData, aes(x = compressionLevel, y = value)) +
  geom_line() +
  facet_grid(metric~., scales = "free")

