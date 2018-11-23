# Open oec-yearly-data.Rproj before running this function

tables <- function(n_cores = 4) {
  # user parameters ---------------------------------------------------------
  
  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets ready to be added to the OEC"
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
  
  # helpers -----------------------------------------------------------------
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  # source("00-scripts/04-download-raw-data.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  # source("00-scripts/06-tidy-downloaded-data.R")
  # source("00-scripts/07-convert-tidy-data-codes.R")
  #source("00-scripts/08-join-converted-datasets.R")
  #Rcpp::sourceCpp("00-scripts/09-proximity-countries-denominator.cpp")
  #Rcpp::sourceCpp("00-scripts/10-proximity-products-denominator.cpp")
  #source("00-scripts/11-compute-rca-and-related-metrics.R")
  source("00-scripts/12-create-final-tables.R")
  
  # codes -------------------------------------------------------------------
  
  load("../ts-comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  load("../ts-comtrade-codes/02-2-tidy-product-data/product-codes.RData")
  load("../ts-observatory-codes/02-2-product-data-tidy/hs-rev2007-product-names.RData")
  
  # pci/eci data ------------------------------------------------------------
  
  eci <- fread2("05-metrics/hs-rev2007-eci/eci-joined-ranking.csv.gz")
  pci <- fread2("05-metrics/hs-rev2007-pci/pci-joined-ranking.csv.gz", char = c("commodity_code"))
  
  pci_4 <- pci %>% filter(commodity_code_length == 4)
  pci_6 <- pci %>% filter(commodity_code_length == 6)
  
  # input data --------------------------------------------------------------
  
  attributes_countries <- country_codes %>% 
    select(iso3_digit_alpha, contains("name"), country_abbrevation) %>% 
    rename(
      country_iso = iso3_digit_alpha,
      country_abbreviation = country_abbrevation
    ) %>% 
    mutate(country_iso = str_to_lower(country_iso)) %>% 
    filter(country_iso != "null") %>% 
    distinct(country_iso, .keep_all = T)
  
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
    )
  
  product_names_2 <- hs_product_names %>% 
    rename(commodity_code = hs) %>% 
    select(-c(product_name,color))
  
  colours <- product_names_2 %>% 
    select(group_id) %>% 
    distinct()
  
  attributes_products <- product_names %>% 
    left_join(product_names_2) %>% 
    rename(group_code = group_id) %>% 
    select(commodity_code, product_fullname_english, group_code, group_name, color)
  
  if (!file.exists(paste0(tables_dir, "/attributes_products.csv.gz"))) {
    fwrite(attributes_products, paste0(tables_dir, "/attributes_products.csv"))
    compress_gz(paste0(tables_dir, "/attributes_products.csv"))
  }
  
  rm(product_names_2)
  
  # tables ------------------------------------------------------------------
  
  if (operating_system != "Windows") {
    mclapply(seq_along(years), compute_tables, mc.cores = n_cores)
  } else {
    lapply(seq_along(years), compute_tables)
  }
}

tables()
