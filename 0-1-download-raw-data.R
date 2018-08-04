# Open oec-yearly-data.Rproj before running this function

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
p_load(data.table, dplyr, jsonlite, doParallel)

download <- function() {
  # user parameters ---------------------------------------------------------

  message(
    "This function eases downloading data from UN Comtrade\nPlease be wise and don't share your tokens or the API could block your access."
  )
  message("\nCopyright (c) 2018, Mauricio \"Pacha\" Vargas\n")
  readline(prompt = "Press [enter] to continue")
  message("\nThe MIT License\n")
  message(
    "Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the \"Software\"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:"
  )
  message(
    "\nThe above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software."
  )
  message(
    "\nTHE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.\n"
  )
  readline(prompt = "Press [enter] to continue")
  
  download_data_rev <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 2"),
    title = "Select dataset:", 
    graphics = T
  )

  remove_old_files <- menu(
    c("yes", "no"), 
    title = "Remove old files (y/n):",
    graphics = T
  )
  
  token <- menu(
    c("yes", "no"), 
    title = "Have you stored your token safely in your .Renviron file?",
    graphics = T
  )
  
  stopifnot(token == 1)
  
  token <- Sys.getenv("token")
  
  # multicore parameters ----------------------------------------------------

  #n_cores <- 4
  n_cores <- detectCores() - 1
  
  # revisions ---------------------------------------------------------------

  download_data_rev1992 <- ifelse(download_data_rev == 1, T, F)
  download_data_rev1996 <- ifelse(download_data_rev == 2, T, F)
  download_data_rev2002 <- ifelse(download_data_rev == 3, T, F)
  download_data_rev2007 <- ifelse(download_data_rev == 4, T, F)
  download_data_rev2 <- ifelse(download_data_rev == 5, T, F)
  
  # HS 1992 -----------------------------------------------------------------

  if (download_data_rev1992 == T) {
    years <- seq(1992, 2016, 1)
    rev <- 1992
    classification <- "H0"
  }
  
  # HS 1996 -----------------------------------------------------------------
  
  if (download_data_rev1996 == T) {
    years <- 1996:2016
    rev <- 1996
    classification <- "H1"
  }
  
  # HS 2002 -----------------------------------------------------------------
  
  if (download_data_rev2002 == T) {
    years <- 2002:2016
    rev <- 2002
    classification <- "H2"
  }
  
  # HS 2007 -----------------------------------------------------------------
  
  if (download_data_rev2007 == T) {
    years <- 2007:2016
    rev <- 2007
    classification <- "H3"
  }
  
  # SITC rev 2 --------------------------------------------------------------
  
  if (download_data_rev2 == T) {
    years <- 1990:2016
    rev <- 2
    classification <- "S2"
  }
  
  # output dirs -------------------------------------------------------------
  
  raw_dir <- "1-1-raw-data"
  
  if (download_data_rev < 5) {
    classification_dir <- sprintf("%s/hs-rev%s", raw_dir, rev)
  } else {
    classification_dir <- sprintf("%s/sitc-rev%s", raw_dir, rev)
  }
  
  try(dir.create(raw_dir))
  try(dir.create(classification_dir))
  
  zip_dir <- paste0(classification_dir, "/zip/")
  
  try(dir.create(zip_dir))
  
  # helpers -----------------------------------------------------------------
  
  data_downloading <- function(t) {
    if (remove_old_files == 1 & 
        (links$local_file_date[[t]] < links$server_file_date[[t]]) & 
        !is.na(links$old_file[[t]])) {
      try(file.remove(links$old_file[[t]]))
    }
    if (!file.exists(links$new_file[[t]])) {
      message(paste("Downloading", links$new_file[[t]]))
      if (links$local_file_date[[t]] < links$server_file_date[[t]]) {
        Sys.sleep(sample(seq(5, 10, by = 1), 1))
        try(
          download.file(links$url[[t]],
                        links$new_file[[t]],
                        method = "wget",
                        quiet = T,
                        extra = "--no-check-certificate")
        )
        
        if(file.size(links$new_file[[t]]) == 0) { fs <- 1 } else { fs <- 0 }
        
        while (fs > 0) {
          try(
            download.file(links$url[[t]],
                          links$new_file[[t]],
                          method = "wget",
                          quiet = T,
                          extra = "--no-check-certificate")
          )
          
          if (file.size(links$new_file[[t]]) == 0) { fs <- fs + 1 } else { fs <- 0 }
        }
      } else {
        message(paste(
          "Existing data is not older than server data. Skipping",
          links$new_file[[t]]
        ))
      }
    } else {
      message(paste(links$new_file[[t]], "exists. Skiping."))
    }
  }
  
  # download data -----------------------------------------------------------
  
  try(
    old_file <- list.files(classification_dir, pattern = "downloaded", full.names = T)
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
  
  links <- tibble(year = years, 
                  url = paste0(
                    "https://comtrade.un.org/api/get/bulk/C/A/",
                    year,
                    "/ALL/",
                    classification,
                    "?token=",
                    token
                  ), 
                  file = NA)
  
  files <- fromJSON(sprintf("https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&px=%s&token=%s", classification, token)) %>% 
    filter(ps %in% years) %>% 
    arrange(ps)
  
  if (exists("old_links")) {
    links <- links %>%
      mutate(file = paste0(zip_dir, files$name)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      left_join(old_links %>% select(-url), by = "year") %>%
      rename(new_file = file) %>% 
      mutate(
        server_file_date = as.Date(
          ifelse(is.na(local_file_date), server_file_date + 1, server_file_date), origin = "1970-01-01"
        ),
        local_file_date = as.Date(
          ifelse(is.na(local_file_date), server_file_date - 1, local_file_date), origin = "1970-01-01"
        )
      )
  } else {
    links <- links %>%
      mutate(file = paste0(zip_dir, files$name)) %>%
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
  
  mclapply(1:length(years), data_downloading, mc.cores = n_cores)
  
  links <- links %>%
    mutate(url = gsub(token, "REPLACE_TOKEN", url)) %>%
    select(year, url, new_file, local_file_date) %>%
    rename(file = new_file)
  
  try(file.remove(old_file))
  fwrite(links, paste0(classification_dir, "/downloaded-files-", Sys.Date(), ".csv"))
}

download()
