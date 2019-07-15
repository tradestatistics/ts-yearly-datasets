# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

complexity <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
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
  source("00-scripts/09-compute-rca-and-related-metrics.R")
  # source("00-scripts/10-create-final-tables.R")

  # RCA based measures ----

  ranking_1 <<- as_tibble(fread("../atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
    mutate(
      iso_code = tolower(iso_code),
      iso_code = ifelse(iso_code == "rou", "rom", iso_code)
    ) %>%
    rename(reporter_iso = iso_code)

  if (operating_system != "Windows") {
    mclapply(seq_along(years_full),
      compute_complexity_measures,
      x = rca_exports_gz,
      yr = eci_rankings_r_gz,
      ye = eci_rankings_e_gz,
      yf = eci_rankings_f_gz,
      zr = pci_rankings_r_gz,
      ze = pci_rankings_e_gz,
      zf = pci_rankings_f_gz,
      q = proximity_countries_gz,
      w = proximity_products_gz,
      n_cores = n_cores
    )
  } else {
    lapply(seq_along(years_full),
      compute_complexity_measures,
      x = rca_exports_gz,
      yr = eci_rankings_r_gz,
      ye = eci_rankings_e_gz,
      yf = eci_rankings_f_gz,
      zr = pci_rankings_r_gz,
      ze = pci_rankings_e_gz,
      zf = pci_rankings_f_gz,
      q = proximity_countries_gz,
      w = proximity_products_gz
    )
  }
  
  # join ECI rankings ----
  
  tidy_eci <- function(d,t) {
    fread2(d) %>% 
      arrange(-value) %>%
      rename(eci = value) %>%
      mutate(
        year = t,
        eci_rank = row_number()
      ) %>%
      select(year, everything())
  }
  
  joined_eci_ranking <- list(
    reflections = map2(eci_rankings_r_gz, years_full, tidy_eci),
    eigenvalues = map2(eci_rankings_e_gz, years_full, tidy_eci),
    fitness = map2(eci_rankings_f_gz, years_full, tidy_eci)
  )
  
  eci_files <- c("/eci-reflections-joined-ranking.csv",
                 "/eci-eigenvalues-joined-ranking.csv",
                 "/eci-fitness-joined-ranking.csv")
  
  write_eci <- function(x,y) {
    d <- bind_rows(joined_eci_ranking[[x]])
    fwrite(d, paste0(eci_dir, y))
    compress_gz(paste0(eci_dir, y))
  }
  
  map2(seq_along(joined_eci_ranking), eci_files, write_eci)
  
  rm(joined_eci_ranking)
  
  # join PCI rankings ----
  
  tidy_pci <- function(d,t) {
    fread2(d, character = c("product")) %>% 
      arrange(-value) %>%
      rename(pci = value) %>%
      mutate(
        year = t,
        pci_rank = row_number()
      ) %>%
      select(year, everything())
  }
  
  joined_pci_ranking <- list(
    reflections = map2(pci_rankings_r_gz, years_full, tidy_pci),
    eigenvalues = map2(pci_rankings_e_gz, years_full, tidy_pci),
    fitness = map2(pci_rankings_f_gz, years_full, tidy_pci)
  )
  
  pci_files <- c("/pci-reflections-joined-ranking.csv",
                 "/pci-eigenvalues-joined-ranking.csv",
                 "/pci-fitness-joined-ranking.csv")
  
  write_pci <- function(x,y) {
    d <- bind_rows(joined_pci_ranking[[x]])
    fwrite(d, paste0(pci_dir, y))
    compress_gz(paste0(pci_dir, y))
  }
  
  map2(seq_along(joined_pci_ranking), pci_files, write_pci)
  
  rm(joined_pci_ranking)
}

complexity()
