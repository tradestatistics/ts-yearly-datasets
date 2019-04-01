# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

tables <- function(n_cores = 4) {
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
  
  load("../comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")
  
  load("../observatory-codes/02-2-product-data-tidy/hs-rev2007-product-names.RData")
  hs_product_names_07 <- hs_product_names
  
  load("../observatory-codes/02-2-product-data-tidy/hs-rev1992-product-names.RData")
  hs_product_names_92 <- hs_product_names %>% 
    select(hs, product_name) %>% 
    rename(
      product_code = hs,
      product_shortname_english = product_name
    )
  
  rm(hs_product_names)
  
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
  
  if (!file.exists(paste0(attributes_dir, "/attributes_countries.csv.gz"))) {
    fwrite(attributes_countries, paste0(attributes_dir, "/attributes_countries.csv"))
    compress_gz(paste0(attributes_dir, "/attributes_countries.csv"))
  }
  
  product_names <- product_codes %>%
    filter(classification == "H3", str_length(code) %in% c(4,6)) %>% 
    select(code, description) %>% 
    rename(
      product_code = code,
      product_fullname_english = description
    ) %>% 
    mutate(group_code = str_sub(product_code, 1, 2))
  
  product_names_2 <- product_codes %>% 
    filter(classification == "H3", str_length(code) == 2) %>% 
    select(code, description) %>% 
    rename(
      group_code = code,
      group_name = description
    )
  
  product_names_3 <- product_names %>%
    left_join(hs_product_names_07, by = c("product_code" = "hs")) %>% 
    rename(
      community_code = group_id,
      community_name = group_name
    ) %>% 
    select(product_code, community_code, community_name)
  
  colours <- product_names_3 %>% 
    select(community_code) %>% 
    distinct() %>% 
    mutate(community_colour = c('#74c0e2', '#406662', '#549e95', '#8abdb6', '#bcd8af', 
                                '#a8c380', '#ede788', '#d6c650', '#dc8e7a', '#d05555',
                                '#bf3251', '#872a41', '#993f7b', '#7454a6', '#a17cb0',
                                '#d1a1bc', '#a1aafb', '#5c57d9', '#1c26b3', '#4d6fd0', 
                                '#7485aa', '#635b56'))
  
  attributes_products <- product_names %>% 
    left_join(product_names_2)

  rm(product_names_2)
  
  if (!file.exists(paste0(attributes_dir, "/attributes_products.csv.gz"))) {
    fwrite(attributes_products, paste0(attributes_dir, "/attributes_products.csv"))
    compress_gz(paste0(attributes_dir, "/attributes_products.csv"))
  }
  
  attributes_communities <- product_names_3 %>% 
    left_join(colours)
  
  if (!file.exists(paste0(attributes_dir, "/attributes_communities.csv.gz"))) {
    fwrite(attributes_communities, paste0(attributes_dir, "/attributes_communities.csv"))
    compress_gz(paste0(attributes_dir, "/attributes_communities.csv"))
  }
  
  rm(product_names_3)
  
  attributes_products_shortnames <- attributes_products %>% 
    select(product_code, product_fullname_english) %>% 
    filter(str_length(product_code) == 4) %>% 
    left_join(hs_product_names_92)
  
  attributes_products_shortnames_complete <- attributes_products_shortnames %>% 
    filter(!is.na(product_shortname_english))
  
  attributes_products_shortnames_nas <- attributes_products_shortnames %>% 
    filter(is.na(product_shortname_english)) %>% 
    mutate(
      product_shortname_english = c(
        "Mercury-based compounds",
        "Miscellaneous inorganic products",
        "Chemical mixtures",
        "Chemical waste",
        "Ovine prepared leather",
        "Non-Ovine prepared leather",
        "Chamois leather",
        "Composition leather",
        "Knitted fabrics, width <= 30 cms",
        "Kniteed fabrics, width > 30 cms",
        "Warp knit fabrics",
        "Other fabrics",
        "Semiconductor machines",
        "Non-electric machinery"
      )
    )
  
  attributes_products_shortnames <- attributes_products_shortnames_complete %>% 
    bind_rows(attributes_products_shortnames_nas) %>% 
    mutate(
      product_shortname_english = iconv(product_shortname_english, from = "", to = "UTF-8"),
      product_shortname_english = ifelse(product_code == "0903", "Mate", product_shortname_english)
    ) %>% 
    arrange(product_code) %>% 
    select(-product_fullname_english)
  
  if (!file.exists(paste0(attributes_dir, "/attributes_products_shortnames.csv.gz"))) {
    fwrite(attributes_products_shortnames, paste0(attributes_dir, "/attributes_products_shortnames.csv"))
    compress_gz(paste0(attributes_dir, "/attributes_products_shortnames.csv"))
  }
  
  rm(attributes_products_shortnames_complete, attributes_products_shortnames_nas)
  
  # tables ------------------------------------------------------------------
  
  if (operating_system != "Windows") {
    mclapply(seq_along(years_full), compute_tables, mc.cores = n_cores)
  } else {
    lapply(seq_along(years_full), compute_tables)
  }
}

tables()
