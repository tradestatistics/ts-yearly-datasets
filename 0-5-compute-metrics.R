# Open ts-yearly-data.Rproj before running this function

metrics <- function(n_cores = 4) {
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
  
  # helpers -----------------------------------------------------------------
  
  source("0-0-helpers.R")
  Rcpp::sourceCpp("0-0-helpers-1.cpp")
  Rcpp::sourceCpp("0-0-helpers-2.cpp")

  # RCA -------------------------------------------------------------------

  if (operating_system != "Windows") {
    rca_exp_and_imp <- c(
      substitute(compute_rca_exports()), 
      substitute(compute_rca_imports())
    )
    mclapply(rca_exp_and_imp, eval, mc.cores= 2)
  } else {
    compute_rca_exports()
    compute_rca_imports()
  }
  
  # RCA based measures ----------------------------------------------------

  ranking_1 <- as_tibble(fread("../ts-atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
    mutate(iso_code = tolower(iso_code)) %>%
    rename(reporter_iso = iso_code)

  if (operating_system != "Windows") {
    mclapply(seq_along(years), compute_rca_metrics,
      x = rca_exports_gz, y = eci_rankings_csv, z = pci_rankings_csv,
      w = proximity_countries_csv, q = proximity_products_csv, r = density_countries_csv, s = density_products_csv, mc.cores = n_cores
    )
  } else {
    lapply(seq_along(years), compute_rca_metrics,
      x = rca_exports_gz, y = eci_rankings_csv, z = pci_rankings_csv,
      w = proximity_countries_csv, q = proximity_products_csv, r = density_countries_csv, s = density_products_csv
    )
  }

  # join ECI rankings -------------------------------------------------------

  joined_eci_rankings <- if (operating_system != "Windows") {
    mclapply(eci_rankings_gz, fread2, mc.cores = n_cores)
  } else {
    lapply(eci_rankings_gz, fread2)
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
  compress_gz(paste0(eci_dir, "/eci-joined-ranking.csv"))
  rm(joined_eci_rankings)

  # join PCI rankings -------------------------------------------------------

  joined_pci_rankings <- if (operating_system != "Windows") {
    mclapply(pci_rankings_gz, fread2, mc.cores = n_cores, char = c("commodity_code"))
  } else {
    lapply(pci_rankings_gz, fread2, char = c("commodity_code"))
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
  compress_gz(paste0(pci_dir, "/pci-joined-ranking.csv"))
  rm(joined_pci_rankings)
}

metrics()
