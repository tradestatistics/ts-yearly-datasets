# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

extract <- function(x, y, t) {
  if (file.exists(str_replace(x[t], "zip", "csv"))) {
    messageline()
    message(paste(x[t], "already unzipped. Skipping."))
  } else {
    messageline()
    message(paste("Unzipping", x[t]))
    system(sprintf("7z e -aos %s -oc:%s", x[t], y))
  }
}

fread2 <- function(file, select = NULL, character = NULL, numeric = NULL) {
  messageline()
  message("function fread2")
  message("file: ", file)

  if (str_sub(file, start = -2) == "gz") {
    d <- fread(
      cmd = paste("zcat", file),
      select = select,
      colClasses = list(
        character = character,
        numeric = numeric
      )
    ) %>%
      as_tibble() %>%
      clean_names()
  } else {
    d <- fread(
      input = file,
      select = select,
      colClasses = list(
        character = character,
        numeric = numeric
      )
    ) %>%
      as_tibble() %>%
      clean_names()
  }

  return(d)
}

file_remove <- function(x) {
  try(file.remove(x))
}

compress_gz <- function(x) {
  system(paste("gzip", x))
}
