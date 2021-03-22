# Open ts-yearly-datasets.Rproj before running this function

download <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2021, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # scripts -----------------------------------------------------------------

  ask_for_token <<- 1
  ask_to_remove_old_files <<- 1
  ask_for_db_access <<- 1
  ask_to_remove_from_db <<- 1
  
  ask_number_of_cores <<- 1
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")
  
  # functions ---------------------------------------------------------------

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
  
  years_to_remove <- files_to_remove$year
  years_to_remove_2 <- unique(c(years_to_remove, years_to_remove + 1, years_to_remove + 2))
    
  # if (operating_system != "Windows") {
  #   mclapply(seq_along(years), data_downloading, dl = download_links, mc.cores = n_cores)
  # } else {
    lapply(seq_along(years), data_downloading, dl = download_links)
  # }

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
    grep(paste(paste0("ps-", years_to_remove), collapse = "|"), ., 
         value = TRUE)
  
  lapply(
    seq_along(years_to_remove),
    function(t) {
      rds <- raw_zip[t] %>% 
        str_replace("/zip/", "/rds/") %>% 
        str_replace("zip$", "rds")
      
      if (!file.exists(rds)) {
        extract(raw_zip[t], raw_dir_rds)
        
        d <- fread(
          input = gsub("\\.rds", "\\.csv", rds),
          colClasses = list(
            character = "Commodity Code",
            numeric = c("Trade Value (US$)", "Qty", "Netweight (kg)")
          ))
        
        saveRDS(d, file = rds, compress = "xz")
        
        rm(d)
        file_remove(gsub("\\.rds", "\\.csv", rds))
      }
      
      if (remove_old_files == 1) {
        try(
          file.remove(
            files_to_remove$old_file[t] %>% 
              str_replace("/zip/", "/rds/") %>% 
              str_replace("zip$", "rds")
          )
        )
      }
    }
  )

  if (remove_old_files == 1) {
    remove_outdated(
      c(clean_rds, converted_rds,
        gsub(paste0(classification, "-rev", revision), "hs-rev2007", unified_rds)),
      years_to_remove_2
    )
    
    remove_outdated(
      c(rca_exports_rds, rca_imports_rds),
      years_to_remove_2
    )
    
    remove_outdated(
      c(proximity_countries_rds, proximity_products_rds),
      years_to_remove_2
    )
    
    remove_outdated(
      c(eci_rankings_f_rds, eci_rankings_r_rds, eci_rankings_e_rds),
      years_to_remove_2
    )
    
    remove_outdated(
      c(pci_rankings_f_rds, pci_rankings_r_rds, pci_rankings_e_rds),
      years_to_remove_2
    )
    
    remove_outdated(
      c(eci_files, pci_files),
      "*"
    )
    
    remove_outdated(
      c(yrpc_rds, yrp_rds, yrc_rds, yr_rds, yc_rds),
      years_to_remove_2
    )
  }
  
  if (remove_from_db == 1) {
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yrpc WHERE year IN ({years_to_remove_2*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yrp WHERE year IN ({years_to_remove_2*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yrc WHERE year IN ({years_to_remove_2*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yr WHERE year IN ({years_to_remove_2*})", .con = con))
    dbGetQuery(con, glue_sql("DELETE FROM public.hs07_yc WHERE year IN ({years_to_remove_2*})", .con = con))
  }
}

download()
