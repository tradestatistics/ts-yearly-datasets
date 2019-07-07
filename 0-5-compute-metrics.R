# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

metrics <- function(n_cores = 4) {
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
  source("00-scripts/09-compute-rca-and-related-metrics.R")
  # source("00-scripts/10-create-final-tables.R")

  # RCA ----

  lapply(seq_along(years_full), compute_rca,
    x = unified_gz, y = rca_exports_gz, group_field = "reporter_iso"
  )

  lapply(seq_along(years_full), compute_rca,
    x = unified_gz, y = rca_imports_gz, group_field = "partner_iso"
  )

  # RCA based measures ----

  ranking_1 <- as_tibble(fread("../atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
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
      w = proximity_products_gz,
      n_cores = 1
    )
  }

  # join ECI rankings ----

  joined_eci_r_rankings <- lapply(eci_rankings_r_gz, fread2)
  joined_eci_e_rankings <- lapply(eci_rankings_e_gz, fread2)
  joined_eci_f_rankings <- lapply(eci_rankings_f_gz, fread2)

  joined_eci_r_rankings <- lapply(
    seq_along(years_full),
    function(t) {
      joined_eci_r_rankings[[t]] %>%
        arrange(-value) %>%
        rename(eci = value) %>%
        mutate(
          year = years_full[t],
          eci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_eci_r_rankings <- bind_rows(joined_eci_r_rankings)
  fwrite(joined_eci_r_rankings, paste0(eci_dir, "/eci-reflections-joined-ranking.csv"))
  compress_gz(paste0(eci_dir, "/eci-reflections-joined-ranking.csv"))
  rm(joined_eci_r_rankings)

  joined_eci_e_rankings <- lapply(
    seq_along(years_full),
    function(t) {
      joined_eci_e_rankings[[t]] %>%
        arrange(-value) %>%
        rename(eci = value) %>%
        mutate(
          year = years_full[t],
          eci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_eci_e_rankings <- bind_rows(joined_eci_e_rankings)
  fwrite(joined_eci_e_rankings, paste0(eci_dir, "/eci-eigenvalues-joined-ranking.csv"))
  compress_gz(paste0(eci_dir, "/eci-eigenvalues-joined-ranking.csv"))
  rm(joined_eci_e_rankings)

  joined_eci_f_rankings <- lapply(
    seq_along(years_full),
    function(t) {
      joined_eci_f_rankings[[t]] %>%
        arrange(-value) %>%
        rename(eci = value) %>%
        mutate(
          year = years_full[t],
          eci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_eci_f_rankings <- bind_rows(joined_eci_f_rankings)
  fwrite(joined_eci_f_rankings, paste0(eci_dir, "/eci-fitness-joined-ranking.csv"))
  compress_gz(paste0(eci_dir, "/eci-fitness-joined-ranking.csv"))
  rm(joined_eci_f_rankings)

  # join PCI rankings ----

  joined_pci_r_rankings <- lapply(pci_rankings_r_gz, fread2, character = c("product"))
  joined_pci_e_rankings <- lapply(pci_rankings_e_gz, fread2, character = c("product"))
  joined_pci_f_rankings <- lapply(pci_rankings_f_gz, fread2, character = c("product"))

  joined_pci_r_rankings_4 <- lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_r_rankings[[t]] %>%
        filter(str_length(product) == 4) %>%
        arrange(-value) %>%
        rename(pci = value) %>%
        mutate(
          year = years_full[t],
          pci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_pci_r_rankings_6 <- lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_r_rankings[[t]] %>%
        filter(str_length(product) == 6) %>%
        arrange(-value) %>%
        rename(pci = value) %>%
        mutate(
          year = years_full[t],
          pci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_pci_r_rankings <- bind_rows(joined_pci_r_rankings_4, joined_pci_r_rankings_6) %>%
    arrange(year, pci_rank)

  fwrite(joined_pci_r_rankings, paste0(pci_dir, "/pci-reflections-joined-ranking.csv"))
  compress_gz(paste0(pci_dir, "/pci-reflections-joined-ranking.csv"))
  rm(joined_pci_r_rankings)

  joined_pci_e_rankings_4 <- lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_e_rankings[[t]] %>%
        filter(str_length(product) == 4) %>%
        arrange(-value) %>%
        rename(pci = value) %>%
        mutate(
          year = years_full[t],
          pci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_pci_e_rankings_6 <- lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_e_rankings[[t]] %>%
        filter(str_length(product) == 6) %>%
        arrange(-value) %>%
        rename(pci = value) %>%
        mutate(
          year = years_full[t],
          pci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_pci_e_rankings <- bind_rows(joined_pci_e_rankings_4, joined_pci_e_rankings_6) %>%
    arrange(year, pci_rank)

  fwrite(joined_pci_e_rankings, paste0(pci_dir, "/pci-eigenvalues-joined-ranking.csv"))
  compress_gz(paste0(pci_dir, "/pci-eigenvalues-joined-ranking.csv"))
  rm(joined_pci_e_rankings)

  joined_pci_f_rankings_4 <- lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_f_rankings[[t]] %>%
        filter(str_length(product) == 4) %>%
        arrange(-value) %>%
        rename(pci = value) %>%
        mutate(
          year = years_full[t],
          pci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_pci_f_rankings_6 <- lapply(
    seq_along(years_full),
    function(t) {
      joined_pci_f_rankings[[t]] %>%
        filter(str_length(product) == 6) %>%
        arrange(-value) %>%
        rename(pci = value) %>%
        mutate(
          year = years_full[t],
          pci_rank = row_number()
        ) %>%
        select(year, everything())
    }
  )

  joined_pci_f_rankings <- bind_rows(joined_pci_f_rankings_4, joined_pci_f_rankings_6) %>%
    arrange(year, pci_rank)

  fwrite(joined_pci_f_rankings, paste0(pci_dir, "/pci-fitness-joined-ranking.csv"))
  compress_gz(paste0(pci_dir, "/pci-fitness-joined-ranking.csv"))
  rm(joined_pci_f_rankings)
}

metrics()
