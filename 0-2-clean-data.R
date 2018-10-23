# Open ts-yearly-data.Rproj before running this function

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[['sysname']]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  pacman::p_load(data.table, dplyr, tidyr, stringr, janitor, purrr, doParallel)
} else {
  pacman::p_load(data.table, dplyr, tidyr, stringr, janitor, purrr)
}

# helpers -----------------------------------------------------------------

source("0-0-helpers.R")

clean <- function(n_cores = 4) {
  # ISO-3 codes -------------------------------------------------------------
  
  load("../ts-comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  
  country_codes <- country_codes %>% 
    select(iso3_digit_alpha) %>% 
    mutate(iso3_digit_alpha = str_to_lower(iso3_digit_alpha)) %>% 
    filter(!iso3_digit_alpha %in% c("wld","null")) %>% 
    as_vector()
  
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

  dataset <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
    title = "Select dataset:",
    graphics = F
  )
  
  # years by classification -------------------------------------------------

  if (dataset < 5) {
    classification <- "hs"
  } else {
    classification <- "sitc"
  }
  
  if (dataset == 1) { revision <- 1992; revision2 <- revision }
  if (dataset == 2) { revision <- 1996; revision2 <- revision }
  if (dataset == 3) { revision <- 2002; revision2 <- revision }
  if (dataset == 4) { revision <- 2007; revision2 <- revision }
  if (dataset == 5) { revision <- 1; revision2 <- 1962 }
  if (dataset == 6) { revision <- 2; revision2 <- 1976 }
  if (dataset == 7) { revision <- 3; revision2 <- 1988 }
  if (dataset == 8) { revision <- 4; revision2 <- 2007 }
  
  years <- revision2:2016

  # number of digits --------------------------------------------------------

  J <- 4
  
  # uncompress input --------------------------------------------------------

  lapply(seq_along(raw_csv), extract, x = raw_zip, y = raw_csv, z = raw_dir)
  
  # create tidy datasets ----------------------------------------------------

  messageline()
  message("Rearranging files. Please wait...")

  # See Anderson & van Wincoop, 2004, Hummels, 2006 and Gaulier & Zignago, 2010 about 8% rate consistency
  cif_fob_rate <- 1.08
  
  if (operating_system != "Windows") {
    mclapply(seq_along(raw_csv), compute_tidy_data, mc.cores = n_cores)
  } else {
    lapply(seq_along(raw_csv), compute_tidy_data)
  }
  
  lapply(raw_csv, file_remove)
}

clean()
