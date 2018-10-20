# Open ts-yearly-data.Rproj before running this function

metrics <- function(n_cores = 4) {
  # detect system -----------------------------------------------------------

  operating_system <- Sys.info()[["sysname"]]

  # packages ----------------------------------------------------------------

  if (!require("pacman")) install.packages("pacman")

  if (operating_system != "Windows") {
    p_load(Matrix, data.table, feather, dplyr, tidyr, stringr, rlang, doParallel)
  } else {
    p_load(Matrix, data.table, feather, dplyr, tidyr, stringr, rlang)
  }

  # helpers -----------------------------------------------------------------

  source("0-0-helpers.R")
  Rcpp::sourceCpp("0-0-helpers-1.cpp")
  Rcpp::sourceCpp("0-0-helpers-2.cpp")

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

  # input dir ---------------------------------------------------------------

  unified_dir <- "03-unified-data/hs-rev2007"

  # output dir --------------------------------------------------------------

  metrics_dir <- "04-metrics"
  try(dir.create(metrics_dir))

  # list input files --------------------------------------------------------

  unified_gz_list <- list.files(path = unified_dir, full.names = T)

  # smooth RCA exports ------------------------------------------------------

  years <- 1962:2016
  years_missing_t_minus_1 <- 1962
  years_missing_t_minus_2 <- 1963

  rca_exports_dir <- sprintf("%s/hs-rev2007-rca-exports", metrics_dir)
  try(dir.create(rca_exports_dir))

  rca_exports_gz <- sprintf("%s/rca-exports-%s.csv.gz", rca_exports_dir, years)
  rca_exports_csv <- gsub(".gz", "", rca_exports_gz)

  if (operating_system != "Windows") {
    mclapply(1:length(years), compute_rca,
      x = unified_gz_list, y = rca_exports_csv, z = rca_exports_gz,
      keep = "reporter_iso", discard = "partner_iso", mc.cores = n_cores
    )
  } else {
    lapply(1:length(years), compute_rca,
      x = unified_gz_list, y = rca_exports_csv, z = rca_exports_gz,
      keep = "reporter_iso", discard = "partner_iso"
    )
  }

  # smooth RCA imports ------------------------------------------------------

  years <- 1962:2016
  years_missing_t_minus_1 <- 1962
  years_missing_t_minus_2 <- 1963

  rca_imports_dir <- sprintf("%s/hs-rev2007-rca-imports", metrics_dir)
  try(dir.create(rca_imports_dir))

  rca_imports_gz <- sprintf("%s/rca-imports-%s.csv.gz", rca_imports_dir, years)
  rca_imports_csv <- gsub(".gz", "", rca_imports_gz)

  if (operating_system != "Windows") {
    mclapply(1:length(years), compute_rca,
      x = unified_gz_list, y = rca_imports_csv, z = rca_imports_gz,
      keep = "partner_iso", discard = "reporter_iso", mc.cores = n_cores
    )
  } else {
    lapply(1:length(years), compute_rca,
      x = unified_gz_list, y = rca_imports_csv, z = rca_imports_gz,
      keep = "partner_iso", discard = "reporter_iso"
    )
  }

  # RCA based measures ----------------------------------------------------

  ranking_1 <- as_tibble(fread("../ts-atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
    mutate(iso_code = tolower(iso_code)) %>%
    rename(reporter_iso = iso_code)

  eci_dir <- sprintf("%s/hs-rev2007-eci", metrics_dir)
  try(dir.create(eci_dir))
  eci_rankings_gz <- sprintf("%s/eci-%s.csv.gz", eci_dir, years)
  eci_rankings_csv <- gsub(".gz", "", eci_rankings_gz)

  pci_dir <- sprintf("%s/hs-rev2007-pci", metrics_dir)
  try(dir.create(pci_dir))
  pci_rankings_gz <- sprintf("%s/pci-%s.csv.gz", pci_dir, years)
  pci_rankings_csv <- gsub(".gz", "", pci_rankings_gz)

  proximity_countries_dir <- sprintf("%s/hs-rev2007-proximity-countries", metrics_dir)
  try(dir.create(proximity_countries_dir))
  proximity_countries_gz <- sprintf("%s/proximity-countries-%s.csv.gz", proximity_countries_dir, years)
  proximity_countries_csv <- gsub(".gz", "", proximity_countries_gz)

  proximity_products_dir <- sprintf("%s/hs-rev2007-proximity-products", metrics_dir)
  try(dir.create(proximity_products_dir))
  proximity_products_gz <- sprintf("%s/proximity-products-%s.csv.gz", proximity_products_dir, years)
  proximity_products_csv <- gsub(".gz", "", proximity_products_gz)

  density_countries_dir <- sprintf("%s/hs-rev2007-density-countries", metrics_dir)
  try(dir.create(density_countries_dir))
  density_countries_gz <- sprintf("%s/density-countries-%s.csv.gz", density_countries_dir, years)
  density_countries_csv <- gsub(".gz", "", density_countries_gz)

  density_products_dir <- sprintf("%s/hs-rev2007-density-products", metrics_dir)
  try(dir.create(density_products_dir))
  density_products_gz <- sprintf("%s/density-products-%s.csv.gz", density_products_dir, years)
  density_products_csv <- gsub(".gz", "", density_products_gz)

  if (operating_system != "Windows") {
    mclapply(1:length(years), compute_rca_metrics,
      x = rca_exports_csv, y = eci_rankings_csv, z = pci_rankings_csv,
      w = proximity_countries_csv, q = proximity_products_csv, r = density_countries_csv, s = density_products_csv, mc.cores = n_cores
    )
  } else {
    lapply(1:length(years), compute_rca_metrics,
      x = rca_exports_csv, y = eci_rankings_csv, z = pci_rankings_csv,
      w = proximity_countries_csv, q = proximity_products_csv, r = density_countries_csv, s = density_products_csv
    )
  }

  # join ECI rankings -------------------------------------------------------

  joined_eci_rankings <- if (operating_system != "Windows") {
    mclapply(eci_rankings_gz, fread5, mc.cores = n_cores)
  } else {
    lapply(eci_rankings_gz, fread5)
  }

  joined_eci_rankings <- if (operating_system != "Windows") {
    mclapply(seq_along(years),
      function(t) {
        joined_eci_rankings[[t]] %>%
          mutate(eci_rank = row_number()) %>%
          select(year, everything())
      },
      mc.cores = n_cores
    )
  } else {
    lapply(
      seq_along(years),
      function(t) {
        joined_eci_rankings[[t]] %>%
          mutate(eci_rank = row_number()) %>%
          select(year, everything())
      }
    )
  }

  joined_eci_rankings <- bind_rows(joined_eci_rankings)
  fwrite(joined_eci_rankings, paste0(eci_dir, "/eci-joined-ranking.csv"))
  compress_gz(paste0(metrics_dir, "/eci-joined-ranking.csv"))
  rm(joined_eci_rankings_full)

  # join PCI rankings -------------------------------------------------------

  joined_pci_rankings <- if (operating_system != "Windows") {
    mclapply(pci_rankings_gz, fread5, mc.cores = n_cores)
  } else {
    lapply(pci_rankings_gz, fread5)
  }

  joined_pci_rankings <- if (operating_system != "Windows") {
    mclapply(seq_along(years),
      function(t) {
        joined_pci_rankings[[t]] %>%
          mutate(pci_rank = row_number()) %>%
          select(year, everything())
      },
      mc.cores = n_cores
    )
  } else {
    lapply(
      seq_along(years),
      function(t) {
        joined_pci_rankings[[t]] %>%
          mutate(pci_rank = row_number()) %>%
          select(year, everything())
      }
    )
  }

  joined_pci_rankings <- bind_rows(joined_pci_rankings)
  fwrite(joined_pci_rankings, paste0(pci_dir, "/pci-joined-ranking.csv"))
  compress_gz(paste0(metrics_dir, "/pci-joined-ranking.csv"))
  rm(joined_pci_rankings_full)
}

metrics()
