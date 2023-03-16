
library(arrow)

# create a FileSystemDataset object
filename <- tempfile(fileext = ".feather")

filename <- here::here("tmp")
write_dataset(cars, filename, format = "feather")
ds <- open_dataset(filename, format = "feather")
ds

# process the file in batches
scanner <- ScannerBuilder$create(ds)$BatchSize(batch_size = 4)$Finish()
reader <- scanner$ToRecordBatchReader()

batch_num <- 1
while(!is.null(batch <- reader$read_next_batch())) {
  print(paste("Reading batch", batch_num, "with", nrow(batch), "rows"))
  batch_num <- batch_num + 1
}

rm(reader)
rm(scanner)
rm(ds)

# remove the file
rc <- unlink(filename, recursive = TRUE)
if(rc == 1) print("removal of file failed")

file.exists(filename)

# call gc()
gc()

# remove the file
rc <- unlink(filename, recursive = TRUE)
if(rc == 1) print("removal of file failed")

file.exists(filename)

rc <- unlink(filename, force = TRUE, recursive = TRUE)
if(rc == 1) print("removal of file failed")

file.exists(filename)
