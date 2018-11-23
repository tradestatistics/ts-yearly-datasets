# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

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

fread2 <- function(x, char = NULL, num = NULL) {
  messageline()
  message("function fread2")
  message("x: ", x)
  
  if (is.null(char) & is.null(num)) {
    d <- fread(
      cmd = paste("zcat", x)
    ) %>%
      as_tibble() %>% 
      clean_names()
  }
  
  if (!is.null(char) & is.null(num)) {
    d <- fread(
      cmd = paste("zcat", x),
      colClasses = list(
        character = char
      )
    ) %>%
      as_tibble() %>% 
      clean_names()
  }
  
  if (!is.null(char) & !is.null(num)) {
    d <- fread(
      cmd = paste("zcat", x),
      colClasses = list(
        character = char,
        numeric = num
      )
    ) %>%
      as_tibble() %>% 
      clean_names()
  }
  
  gc()
  return(d)
}

file_remove <- function(x) {
  try(file.remove(x))
}

compress_gz <- function(x) {
  system(paste("gzip", x))
}
