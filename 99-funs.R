# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

messageline <- function(txt = NULL, width = 80) {
  if(is.null(txt)) {
    message(rep("-", width))
  } else {
    message(txt, " ", rep("-", width - nchar(txt) - 1))
  }
}

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

file_remove <- function(x) {
  try(file.remove(x))
}

remove_outdated <- function(x,t) {
  try(
    file.remove(
      grep(
        paste(paste0(t, ".csv.gz"), collapse = "|"),
        x,
        value = T
      )
    )
  )
}
