# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

data_downloading <- function(t) {
  if (remove_old_files == 1 &
    (links$local_file_date[t] < links$server_file_date[t]) &
    !is.na(links$old_file[t])) {
    try(file.remove(links$old_file[t]))
  }
  if (!file.exists(links$new_file[t])) {
    message(paste("Downloading", links$new_file[t]))
    if (links$local_file_date[t] < links$server_file_date[t]) {
      Sys.sleep(sample(seq(5, 10, by = 1), 1))
      try(
        download.file(links$url[t],
          links$new_file[t],
          method = "wget",
          quiet = T,
          extra = "--no-check-certificate"
        )
      )

      if (file.size(links$new_file[t]) == 0) {
        fs <- 1
      } else {
        fs <- 0
      }

      while (fs > 0) {
        try(
          download.file(links$url[t],
            links$new_file[t],
            method = "wget",
            quiet = T,
            extra = "--no-check-certificate"
          )
        )

        if (file.size(links$new_file[t]) == 0) {
          fs <- fs + 1
        } else {
          fs <- 0
        }
      }
    } else {
      message(paste(
        "Existing data is not older than server data. Skipping",
        links$new_file[t]
      ))
    }
  } else {
    message(paste(links$new_file[t], "exists. Skiping."))
  }
}
