# Open ts-yearly-data.Rproj before running this function

clean <- function(n_cores = 4) {
  # detect system -----------------------------------------------------------
  
  operating_system <- Sys.info()[['sysname']]
  
  # packages ----------------------------------------------------------------
  
  if (!require("pacman")) install.packages("pacman")
  
  if (operating_system != "Windows") {
    pacman::p_load(data.table, dplyr, tidyr, stringr, janitor, purrr, doParallel)
  } else {
    pacman::p_load(data.table, dplyr, tidyr, stringr, janitor, purrr)
  }
  
  # helpers -----------------------------------------------------------------
  
  source("0-0-helpers.R")
  
  # ISO-3 codes -------------------------------------------------------------
  
  load("../ts-comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  
  country_codes <- country_codes %>% 
    select(iso3_digit_alpha) %>% 
    mutate(iso3_digit_alpha = str_to_lower(iso3_digit_alpha)) %>% 
    filter(!iso3_digit_alpha %in% c("wld","null")) %>% 
    as_vector()
  
  # user parameters ---------------------------------------------------------

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

  dataset <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
    title = "Select dataset:",
    graphics = F
  )
  
  # years by classification -------------------------------------------------

  if (dataset < 5) {
    classification <- "hs"
  } else {
    classification <- "sitc"
  }
  
  if (dataset == 1) { rev <- 1992; rev2 <- rev }
  if (dataset == 2) { rev <- 1996; rev2 <- rev }
  if (dataset == 3) { rev <- 2002; rev2 <- rev }
  if (dataset == 4) { rev <- 2007; rev2 <- rev }
  if (dataset == 5) { rev <- 1; rev2 <- 1962 }
  if (dataset == 6) { rev <- 2; rev2 <- 1976 }
  if (dataset == 7) { rev <- 3; rev2 <- 1988 }
  if (dataset == 8) { rev <- 4; rev2 <- 2007 }
  
  years <- rev2:2016

  # number of digits --------------------------------------------------------

  J <- 4
  
  # input dirs --------------------------------------------------------------

  raw_dir <- sprintf("01-raw-data/%s-rev%s", classification, rev)
  
  # output dirs -------------------------------------------------------------

  clean_dir <- "02-clean-data"
  rev_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, rev)
  try(dir.create(clean_dir))
  try(dir.create(rev_dir))

  # list input files --------------------------------------------------------

  raw_zip_list <-
    list.files(
      path = raw_dir,
      pattern = "\\.zip",
      full.names = T
    ) %>%
    grep(paste(paste0("ps-", years), collapse = "|"), ., value = TRUE)

  raw_csv_list <- raw_zip_list %>% gsub("zip", "csv", .)

  # list output files -------------------------------------------------------

  clean_csv_list <- sprintf("%s/%s-rev%s-%s.csv", rev_dir, classification, rev, years)
  clean_gz_list <- sprintf("%s/%s-rev%s-%s.csv.gz", rev_dir, classification, rev, years)

  # uncompress input --------------------------------------------------------

  lapply(seq_along(raw_csv_list), extract, x = raw_zip_list, y = raw_csv_list, z = raw_dir)
  
  # create tidy datasets ----------------------------------------------------

  messageline()
  message("Rearranging files. Please wait...")

  # See Anderson & van Wincoop, 2004, Hummels, 2006 and Gaulier & Zignago, 2010 about 8% rate consistency
  cif_fob_rate <- 1.08

  compute_tidy_data <- function(t) {
    if (!file.exists(clean_gz_list[[t]])) {
      messageline()
      message(paste("Cleaning", years[[t]], "data..."))

      # clean data --------------------------------------------------------------

      clean_data <- fread2(raw_csv_list[[t]]) %>%
        rename(trade_value_usd = trade_value_us) %>%
        select(trade_flow, reporter_iso, partner_iso, aggregate_level, commodity_code, trade_value_usd) %>%
        
        filter(aggregate_level %in% J) %>%
        filter(trade_flow %in% c("Export","Import")) %>%
        
        filter(
          !is.na(commodity_code),
          commodity_code != "",
          commodity_code != " "
        ) %>%
        
        mutate(
          reporter_iso = str_to_lower(reporter_iso),
          partner_iso = str_to_lower(partner_iso)
        ) %>%
        
        filter(
          reporter_iso %in% country_codes,
          partner_iso %in% country_codes
        )

      # exports data ------------------------------------------------------------

      exports <- clean_data %>%
        filter(trade_flow == "Export") %>%
        unite(pairs, reporter_iso, partner_iso, commodity_code, sep = "_", remove = F) %>%
        select(pairs, trade_value_usd) %>% 
        mutate(trade_value_usd = ceiling(trade_value_usd))

      exports_mirrored <- clean_data %>%
        filter(trade_flow == "Import") %>%
        unite(pairs, partner_iso, reporter_iso, commodity_code, sep = "_", remove = F) %>%
        select(pairs, trade_value_usd) %>% 
        mutate(trade_value_usd = ceiling(trade_value_usd / cif_fob_rate))

      rm(clean_data)
      
      exports_model <- exports %>% 
        full_join(exports_mirrored, by = "pairs") %>% 
        rowwise() %>% 
        mutate(trade_value_usd = max(trade_value_usd.x, trade_value_usd.y, na.rm = T)) %>% 
        ungroup() %>% 
        separate(pairs, c("reporter_iso", "partner_iso", "commodity_code"), sep = "_") %>%
        mutate(year = years[[t]]) %>%
        select(year, everything(), -ends_with("x"), -ends_with("y"))
        
      rm(exports, exports_mirrored)

      fwrite(exports_model, clean_csv_list[[t]])
      compress_gz(clean_csv_list[[t]])
    } else {
      messageline()
      message(paste("Skipping year", years[[t]], "Files exist."))
    }
  }

  if (operating_system != "Windows") {
    mclapply(seq_along(raw_csv_list), compute_tidy_data, mc.cores = n_cores)
  } else {
    lapply(seq_along(raw_csv_list), compute_tidy_data)
  }
  
  lapply(raw_csv_list, file_remove)
}

clean()
