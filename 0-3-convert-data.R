# Open ts-yearly-data.Rproj before running this function

# user input --------------------------------------------------------------

ask_for_convertion_codes <- 1

convert <- function(n_cores = 4) {
  # messages ----------------------------------------------------------------
  
  message("\nCopyright (C) 2018, Mauricio \"Pacha\" Vargas\n")
  message("This file is part of Open Trade Statistics project")
  message("\nThe scripts within this project are released under GNU General Public License 3.0")
  message("This program comes with ABSOLUTELY NO WARRANTY.")
  message("This is free software, and you are welcome to redistribute it under certain conditions.\n")
  message("See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details\n")
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")
  
  # scripts -----------------------------------------------------------------
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  # source("00-scripts/04-download-raw-data.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  # source("00-scripts/06-tidy-downloaded-data.R")
  source("00-scripts/07-convert-tidy-data-codes.R")
  # source("00-scripts/08-join-converted-datasets.R")
  # Rcpp::sourceCpp("00-scripts/09-proximity-countries-denominator.cpp")
  # Rcpp::sourceCpp("00-scripts/10-proximity-products-denominator.cpp")
  # source("00-scripts/11-compute-rca-and-related-metrics.R")
  # source("00-scripts/12-create-final-tables.R")
  
  # convert data ------------------------------------------------------------

  if (operating_system != "Windows") {
    mclapply(seq_along(converted_gz), convert_codes, mc.cores = n_cores, 
             x = clean_gz, y = converted_gz)
  } else {
    lapply(seq_along(converted_gz), convert_codes, 
           x = clean_gz, y = converted_gz)
  }
}

convert()
