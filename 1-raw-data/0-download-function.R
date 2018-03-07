# Open oec-yearly-datasets.Rproj before running this function

download <- function() {

  # user parameters ---------------------------------------------------------

  message(
    "This function eases downloading data from UN Comtrade\nPlease be wise and don't share your tokens or the API could block your access."
  )
  message("\nCopyright (c) 2017, Datawheel\n")
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
  
  message("Paste UN COMTRADE token (or press ESC if you don't have a token)")
  token <- readline(prompt = "token: ")
  
  message(
    "\n (1) HS rev 1992\n (2) HS rev 1996\n (3) HS rev 2002\n (4) HS rev 2007\n (5) HS rev 2012\n (6) SITC rev 2 "
  )
  
  download_data_rev <- readline(prompt = "Select dataset: ")

  remove_old_files <- readline(prompt = "Remove old files (y/n): ")
  
  # input validation --------------------------------------------------------
  
  if (!download_data_rev %in% 1:6) {
    stop("Only numeric values are valid for dataset selection.")
  }
  
  if (!download_data_rev %in% c("y","n")) {
    stop("Only yes/no values are valid for remove old files option.")
  }
  
  # packages ----------------------------------------------------------------

  if (!require("pacman")) install.packages("pacman")
  p_load(data.table, dplyr, doParallel)
  
  # multicore parameters ----------------------------------------------------

  #n_cores <- 4
  n_cores <- ceiling(detectCores() - 1)
  
  # revisions ---------------------------------------------------------------

  download_data_rev1992 <- ifelse(download_data_rev == 1, T, F)
  download_data_rev1996 <- ifelse(download_data_rev == 2, T, F)
  download_data_rev2002 <- ifelse(download_data_rev == 3, T, F)
  download_data_rev2007 <- ifelse(download_data_rev == 4, T, F)
  download_data_rev2012 <- ifelse(download_data_rev == 5, T, F)
  download_data_rev2 <- ifelse(download_data_rev == 6, T, F)
  
  # directories -------------------------------------------------------------

  raw_dir <- "1-raw-data/"
  
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
  
  # HS 2012 -----------------------------------------------------------------
  
  if (download_data_rev2012 == T) {
    years <- 2012:2016
    rev <- 2012
    classification <- "H4"
  }
  
  # SITC rev 2 --------------------------------------------------------------
  
  if (download_data_rev2 == T) {
    years <- 2000:2016
    rev <- 2
    classification <- "S2"
  }
  
  # download trade data -----------------------------------------------------
  
  if (download_data_rev < 6) {
    classification_dir <- paste0(raw_dir, "hs-rev", rev)
  } else {
    classification_dir <- paste0(raw_dir, "sitc-rev", rev)
  }
  
  try(dir.create(classification_dir))
  
  zip_dir <- paste0(classification_dir, "zip/")
  csv_dir <- paste0(classification_dir, "csv/")
  
  try(dir.create(zip_dir))
  
  # helpers -----------------------------------------------------------------

  get_filenames <- function(t) {
    Sys.sleep(sample(seq(2, 4, by=1), 1))
    system(
      paste(
        "curl -IXGET -r 0-10",
        links$url[[t]],
        "| grep attachment | sed 's/^.\\+filename=//'"
      ),
      intern = T
    )
  }
  
  data_downloading <- function(t) {
    Sys.sleep(sample(seq(2, 4, by=1), 1))
    if (remove_old_files == "y" & (links$local_file_date[[t]] < links$server_file_date[[t]]) & !is.na(links$old_file[[t]])) {
      try(file.remove(links$old_file[[t]]))
    }
    if (!file.exists(links$new_file[[t]])) {
      message(paste("Downloading", links$new_file[[t]]))
      if (links$local_file_date[[t]] < links$server_file_date[[t]]) {
        try(download.file(links$url[[t]],
                          links$new_file[[t]],
                          method = "wget",
                          extra = "--no-check-certificate"))
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

  if (file.exists(paste0(classification_dir, "downloaded-files-", Sys.Date(), ".csv"))) {
    old_links <- as_tibble(fread(paste0(
      classification_dir, "downloaded-files-", Sys.Date(), ".csv"
    ))) %>%
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
  
  file <- mclapply(1:length(years), get_filenames, mc.cores = n_cores)
  
  links$file <- as.character(file); rm(file)
  
  if (file.exists(paste0(classification_dir, "downloaded-files-", Sys.Date(), ".csv"))) {
    links <- links %>%
      mutate(file = paste0(zip_dir, file),
             file = gsub("\r", "", file)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      left_join(old_links %>% select(-url), by = "year") %>%
      rename(new_file = file)
  } else {
    links <- links %>%
      mutate(file = paste0(zip_dir, file),
             file = gsub("\r", "", file)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      mutate(old_file = NA) %>% # trick in case there are no old files 
      mutate(local_file_date = server_file_date) %>% 
      mutate(server_file_date = server_file_date + 1) %>% # trick in case there are no old files 
      rename(new_file = file)
  }
  
  mclapply(1:length(years), data_downloading, mc.cores = n_cores)
  
  links <- links %>%
    mutate(url = gsub(token, "REPLACE_TOKEN", url)) %>%
    select(year, url, new_file, local_file_date) %>%
    rename(file = new_file)
  
  fwrite(links, paste0(classification_dir, "downloaded-files-", Sys.Date(), ".csv"))
  
}

download()
