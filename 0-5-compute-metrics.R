# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

metrics <- function(n_cores = 2) {
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
  #source("00-scripts/08-join-converted-datasets.R")
  Rcpp::sourceCpp("00-scripts/09-proximity-countries-denominator.cpp")
  Rcpp::sourceCpp("00-scripts/10-proximity-products-denominator.cpp")
  source("00-scripts/11-compute-rca-and-related-metrics.R")
  # source("00-scripts/12-create-final-tables.R")

  # RCA -------------------------------------------------------------------

  lapply(seq_along(years_full), compute_rca,
         x = unified_gz, y = rca_exports_gz, keep = "reporter_iso"
  )
  
  lapply(seq_along(years_full), compute_rca,
         x = unified_gz, y = rca_imports_gz, keep = "partner_iso"
  )
  
  # RCA based measures ----------------------------------------------------
  
  ranking_1 <- as_tibble(fread("../ts-atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
    mutate(iso_code = tolower(iso_code)) %>%
    rename(reporter_iso = iso_code)
  
  if (operating_system != "Windows") {
    mclapply(seq_along(years_full), compute_rca_metrics,
             x = rca_exports_gz, y = eci_rankings_gz, z = pci_rankings_gz,
             q = proximity_countries_gz, w = proximity_products_gz, n_cores = n_cores
    )
  } else {
    lapply(seq_along(years_full), compute_rca_metrics,
           x = rca_exports_gz, y = eci_rankings_gz, z = pci_rankings_gz,
           q = proximity_countries_gz, w = proximity_products_gz
    ) 
  }
  
  # join ECI rankings -------------------------------------------------------

  joined_eci_rankings <- lapply(eci_rankings_gz, fread2)

  joined_eci_rankings <- lapply(
      seq_along(years_full),
      function(t) {
        joined_eci_rankings[[t]] %>%
          arrange(-eci) %>% 
          mutate(eci_rank = row_number()) %>%
          select(year, everything())
      }
    )

  joined_eci_rankings <- bind_rows(joined_eci_rankings)
  fwrite(joined_eci_rankings, paste0(eci_dir, "/eci-joined-ranking.csv"))
  compress_gz(paste0(eci_dir, "/eci-joined-ranking.csv"))
  rm(joined_eci_rankings)

  # join PCI rankings -------------------------------------------------------

  joined_pci_rankings <- lapply(pci_rankings_gz, fread2, character = c("commodity_code"))
  
  joined_pci_rankings <-  lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_rankings[[t]] %>%
        mutate(commodity_code_length = str_length(commodity_code)) %>% 
        arrange(-pci, commodity_code_length) %>% 
        group_by(commodity_code_length) %>% 
        mutate(pci_rank = row_number()) %>%
        ungroup()
    }
  )

  joined_pci_rankings <- bind_rows(joined_pci_rankings)
  fwrite(joined_pci_rankings, paste0(pci_dir, "/pci-joined-ranking.csv"))
  compress_gz(paste0(pci_dir, "/pci-joined-ranking.csv"))
  rm(joined_pci_rankings)
}

metrics()
