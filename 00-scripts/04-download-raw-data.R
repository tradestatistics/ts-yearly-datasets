# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

data_downloading <- function(t,dl) {
  if (remove_old_files == 1 &
    (dl$local_file_date[t] < dl$server_file_date[t]) &
    !is.na(dl$old_file[t])) {
    try(file.remove(dl$old_file[t]))
  }
  if (!file.exists(dl$new_file[t])) {
    message(paste("Downloading", dl$new_file[t]))
    if (dl$local_file_date[t] < dl$server_file_date[t]) {
      Sys.sleep(sample(seq(5, 10, by = 1), 1))
      try(
        download.file(dl$url[t],
          dl$new_file[t],
          method = "wget",
          quiet = T,
          extra = "--no-check-certificate"
        )
      )

      if (file.size(dl$new_file[t]) == 0) {
        fs <- 1
      } else {
        fs <- 0
      }

      while (fs > 0) {
        try(
          download.file(dl$url[t],
            dl$new_file[t],
            method = "wget",
            quiet = T,
            extra = "--no-check-certificate"
          )
        )

        if (file.size(dl$new_file[t]) == 0) {
          fs <- fs + 1
        } else {
          fs <- 0
        }
      }
    } else {
      message(paste(
        "Existing data is not older than server data. Skipping",
        dl$new_file[t]
      ))
    }
  } else {
    message(paste(dl$new_file[t], "exists. Skiping."))
  }
}
