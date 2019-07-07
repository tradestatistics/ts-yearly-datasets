# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

# user input --------------------------------------------------------------

ask_for_token <- 1
ask_to_remove_old_files <- 1

download <- function(n_cores = 4) {
  # messages ----------------------------------------------------------------

  message("\nCopyright (C) 2018, Mauricio \"Pacha\" Vargas\n")
  message("This file is part of Open Trade Statistics project")
  message("\nThe scripts within this project are released under GNU General Public License 3.0")
  message("This program comes with ABSOLUTELY NO WARRANTY.")
  message("This is free software, and you are welcome to redistribute it under certain conditions.\n")
  message("See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details\n")
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # scripts -----------------------------------------------------------------

  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  source("00-scripts/04-download-raw-data.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  # source("00-scripts/06-tidy-downloaded-data.R")
  # source("00-scripts/07-convert-tidy-data-codes.R")
  # source("00-scripts/08-join-converted-datasets.R")
  # source("00-scripts/09-compute-rca-and-related-metrics.R")
  # source("00-scripts/10-create-final-tables.R")

  # download data -----------------------------------------------------------

  try(
    old_file <- list.files(raw_dir, pattern = "downloaded", full.names = T)
  )

  if (length(old_file) > 0) {
    old_links <- as_tibble(fread(old_file)) %>%
      mutate(
        local_file_date = gsub(".*pub-", "", file),
        local_file_date = gsub("_fmt.*", "", local_file_date),
        local_file_date = as.Date(local_file_date, "%Y%m%d")
      ) %>%
      rename(old_file = file)
  }

  links <- tibble(
    year = years,
    url = paste0(
      "https://comtrade.un.org/api/get/bulk/C/A/",
      year,
      "/ALL/",
      classification2,
      "?token=",
      Sys.getenv("token")
    ),
    file = NA
  )

  files <- fromJSON(sprintf("https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&px=%s&token=%s", classification2, token)) %>%
    filter(ps %in% years) %>%
    arrange(ps)

  if (exists("old_links")) {
    links <- links %>%
      mutate(file = paste0(raw_dir_zip, "/", files$name)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      left_join(old_links %>% select(-url), by = "year") %>%
      rename(new_file = file) %>%
      mutate(
        server_file_date = as.Date(
          ifelse(is.na(local_file_date), server_file_date + 1, server_file_date),
          origin = "1970-01-01"
        ),
        local_file_date = as.Date(
          ifelse(is.na(local_file_date), server_file_date - 1, local_file_date),
          origin = "1970-01-01"
        )
      )
  } else {
    links <- links %>%
      mutate(file = paste0(raw_dir_zip, "/", files$name)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      mutate(old_file = NA) %>% # trick in case there are no old files
      mutate(local_file_date = server_file_date) %>%
      mutate(server_file_date = as.Date(server_file_date + 1, origin = "1970-01-01")) %>% # trick in case there are no old files
      rename(new_file = file)
  }

  if (operating_system != "Windows") {
    mclapply(seq_along(years), data_downloading, mc.cores = n_cores)
  } else {
    lapply(seq_along(years), data_downloading)
  }

  links <- links %>%
    mutate(url = str_replace(url, "token=*", "token=REPLACE_TOKEN")) %>%
    select(year, url, new_file, local_file_date) %>%
    rename(file = new_file)

  try(file.remove(old_file))
  fwrite(links, paste0(raw_dir, "/downloaded-files-", Sys.Date(), ".csv"))

  # re-compress -------------------------------------------------------------

  lapply(
    seq_along(raw_zip),
    function(t) {
      gz <- raw_zip[t] %>% str_replace("/zip/", "/gz/") %>% str_replace("zip$", "csv.gz")

      if (!file.exists(gz)) {
        x <- raw_zip[t]
        extract(x, y = raw_dir_gz)
      }
    }
  )

  raw_csv <- list.files(path = raw_dir_gz, pattern = "csv$", full.names = T)

  lapply(
    seq_along(raw_csv),
    function(t) {
      x <- raw_csv[t]
      y <- str_replace(raw_csv[t], "csv$", "csv.gz")
      if (!file.exists(y)) {
        compress_gz(x)
      }
    }
  )
}

download()
