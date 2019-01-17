# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

tables <- function(n_cores = 2) {
  # messages ----------------------------------------------------------------
  
  message("\nCopyright (C) 2018, Mauricio \"Pacha\" Vargas\n")
  message("This file is part of Open Trade Statistics project")
  message("\nThe scripts within this project are released under GNU General Public License 3.0")
  message("This program comes with ABSOLUTELY NO WARRANTY.")
  message("This is free software, and you are welcome to redistribute it under certain conditions.\n")
  message("See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details\n")
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")
  
  # helpers -----------------------------------------------------------------
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  # source("00-scripts/04-download-raw-data.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  # source("00-scripts/06-tidy-downloaded-data.R")
  # source("00-scripts/07-convert-tidy-data-codes.R")
  # source("00-scripts/08-join-converted-datasets.R")
  # Rcpp::sourceCpp("00-scripts/09-proximity-countries-denominator.cpp")
  # Rcpp::sourceCpp("00-scripts/10-proximity-products-denominator.cpp")
  # source("00-scripts/11-compute-rca-and-related-metrics.R")
  source("00-scripts/12-create-final-tables.R")
  
  # codes -------------------------------------------------------------------
  
  load("../ts-comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  load("../ts-comtrade-codes/02-2-tidy-product-data/product-codes.RData")

  # input data --------------------------------------------------------------
  
  attributes_countries <- country_codes %>% 
    select(
      iso3_digit_alpha, contains("name"), country_abbrevation, 
      contains("continent"), eu28_member
    ) %>% 
    rename(
      country_iso = iso3_digit_alpha,
      country_abbreviation = country_abbrevation
    ) %>% 
    mutate(country_iso = str_to_lower(country_iso)) %>% 
    filter(country_iso != "null") %>% 
    distinct(country_iso, .keep_all = T) %>% 
    select(-country_abbreviation)
  
  continents <- attributes_countries %>% 
    select(continent_id) %>% 
    distinct()
  
  attributes_countries <- attributes_countries %>% 
    left_join(continents)
  
  if (!file.exists(paste0(tables_dir, "/attributes_countries.csv.gz"))) {
    fwrite(attributes_countries, paste0(tables_dir, "/attributes_countries.csv"))
    compress_gz(paste0(tables_dir, "/attributes_countries.csv"))
  }
  
  product_names <- product_codes %>%
    filter(classification == "H3", str_length(code) %in% c(4,6)) %>% 
    select(code, description) %>% 
    rename(
      commodity_code = code,
      product_fullname_english = description
    ) %>% 
    mutate(group_code = str_sub(commodity_code, 1, 2))
  
  product_names_2 <- product_codes %>% 
    filter(classification == "H3", str_length(code) == 2) %>% 
    select(code, description) %>% 
    rename(
      group_code = code,
      group_name = description
    )
  
  attributes_products <- product_names %>% 
    left_join(product_names_2)

  if (!file.exists(paste0(tables_dir, "/attributes_products.csv.gz"))) {
    fwrite(attributes_products, paste0(tables_dir, "/attributes_products.csv"))
    compress_gz(paste0(tables_dir, "/attributes_products.csv"))
  }
  
  rm(product_names_2)
  
  # tables ------------------------------------------------------------------
  
  if (operating_system != "Windows") {
    mclapply(seq_along(years_full), compute_tables, mc.cores = n_cores)
  } else {
    lapply(seq_along(years_full), compute_tables)
  }
}

tables()
