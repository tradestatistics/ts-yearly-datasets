# Open ts-yearly-data.Rproj before running this function

metrics <- function(n_cores = 2) {
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

  if (operating_system != "Windows") {
    rca_exp_and_imp <- c(
      substitute(compute_rca_exports()), 
      substitute(compute_rca_imports())
    )
    mclapply(rca_exp_and_imp, eval, mc.cores = n_cores)
  } else {
    compute_rca_exports()
    compute_rca_imports()
  }
  
  # RCA based measures ----------------------------------------------------
  
  ranking_1 <- as_tibble(fread("../ts-atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
    mutate(iso_code = tolower(iso_code)) %>%
    rename(reporter_iso = iso_code)

  lapply(seq_along(years_full), compute_rca_metrics,
         x = rca_exports_gz, y = eci_rankings_gz, z = pci_rankings_gz,
         q = proximity_countries_gz, w = proximity_products_gz, n_cores = n_cores
  )
  
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

  joined_pci_rankings <- lapply(pci_rankings_gz, fread2, char = c("commodity_code"))
  
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
