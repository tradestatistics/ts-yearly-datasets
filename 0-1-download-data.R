# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

download <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # scripts -----------------------------------------------------------------

  ask_for_token <<- 1
  ask_to_remove_old_files <<- 1
  ask_for_db_access <<- 1
  ask_to_remove_from_db <<- 1
  
  ask_number_of_cores <<- 1
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  source("00-scripts/04-download-raw-data.R")
  source("00-scripts/05-read-extract-remove-compress.R")

  # download data -----------------------------------------------------------

  try(
    old_file <- list.files(raw_dir, pattern = "downloaded", full.names = T)
  )

  if (length(old_file) > 0) {
    old_download_links <- as_tibble(fread(old_file)) %>%
      mutate(
        local_file_date = gsub(".*pub-", "", file),
        local_file_date = gsub("_fmt.*", "", local_file_date),
        local_file_date = as.Date(local_file_date, "%Y%m%d")
      ) %>%
      rename(old_file = file)
  }

  download_links <- tibble(
    year = years,
    url = paste0(
      "https://comtrade.un.org/api/get/bulk/C/A/",
      year,
      "/ALL/",
      classification2,
      "?token=",
      token
    ),
    file = NA
  )

  files <- fromJSON(sprintf("https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&px=%s&token=%s", classification2, token)) %>%
    filter(ps %in% years) %>%
    arrange(ps)

  if (exists("old_download_links")) {
    download_links <- download_links %>%
      mutate(
        file = paste0(raw_dir_zip, "/", files$name),
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      left_join(old_download_links %>% select(-url), by = "year") %>%
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
    download_links <- download_links %>%
      mutate(
        file = paste0(raw_dir_zip, "/", files$name),
    
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d"),
        
        # trick in case there are no old files
        old_file = NA,
        
        local_file_date = server_file_date,
        server_file_date = as.Date(server_file_date + 1, origin = "1970-01-01")
      ) %>% 
      rename(new_file = file)
  }

  files_to_remove <- download_links %>% 
    filter(local_file_date < server_file_date)
    
  if (operating_system != "Windows") {
    mclapply(seq_along(years), data_downloading, dl = download_links, mc.cores = n_cores)
  } else {
    lapply(seq_along(years), data_downloading, dl = download_links)
  }

  download_links <- download_links %>%
    select(year, url, new_file, local_file_date) %>%
    rename(file = new_file)
  
  download_links <- download_links %>% 
    mutate(url = str_replace_all(url, "token=.*", "token=REPLACE_TOKEN"))
  
  try(file.remove(old_file))
  fwrite(download_links, paste0(raw_dir, "/downloaded-files-", Sys.Date(), ".csv"))

  # re-compress -------------------------------------------------------------

  raw_zip <- list.files(
    path = raw_dir_zip,
    pattern = "\\.zip",
    full.names = T
  ) %>%
    grep(paste(paste0("ps-", files_to_remove$year), collapse = "|"), ., 
         value = TRUE)
  
  lapply(
    seq_along(files_to_remove$year),
    function(t) {
      gz <- raw_zip[t] %>% str_replace("/zip/", "/gz/") %>% 
        str_replace("zip$", "csv.gz")
      
      if (!file.exists(gz)) {
        extract(raw_zip[t], raw_dir_gz)
        compress_gz(str_replace(gz, "csv.gz", "csv"))
      }
      
      try(
        file.remove(
          files_to_remove$old_file[t] %>% str_replace("/zip/", "/gz/") %>% 
            str_replace("zip$", "csv.gz")
        )
      )
    }
  )

  if (remove_old_files == 1) {
    lapply(c(clean_gz, converted_gz, unified_gz), remove_outdated)
    lapply(c(eci_rankings_f_gz, eci_rankings_r_gz, eci_rankings_e_gz), remove_outdated)
    lapply(c(pci_rankings_f_gz, pci_rankings_r_gz, pci_rankings_e_gz), remove_outdated)
    lapply(c(proximity_countries_gz, proximity_products_gz), remove_outdated)
    lapply(c(rca_exports_gz, rca_imports_gz), remove_outdated)
    lapply(c(yrpc_gz, yrp_gz, yrc_gz, yr_gz, yc_gz), remove_outdated)
  }
  
  remove_outdated_2(c(rca_exports_gz, rca_imports_gz), files_to_remove$year + 1)
  remove_outdated_2(c(rca_exports_gz, rca_imports_gz), files_to_remove$year + 2)
  
  if (remove_from_db == 1) {
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yrpc WHERE year IN ({files_to_remove$year*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yrp WHERE year IN ({files_to_remove$year*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yrc WHERE year IN ({files_to_remove$year*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yr WHERE year IN ({files_to_remove$year*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yc WHERE year IN ({files_to_remove$year*})", .con = con))
  }
}

download()
