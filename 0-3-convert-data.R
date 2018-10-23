# Open ts-yearly-data.Rproj before running this function

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[['sysname']]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  pacman::p_load(data.table, dplyr, stringr, rlang, doParallel)
} else {
  pacman::p_load(data.table, dplyr, stringr, rlang)
}

# helpers -----------------------------------------------------------------

source("0-0-helpers.R")

# product codes -----------------------------------------------------------

load("../ts-comtrade-codes/02-2-tidy-product-data/product-correspondence.RData")

convert <- function(n_cores = 4) {
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
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "SITC rev 1", "SITC rev 2"),
    title = "Select dataset:",
    graphics = F
  )
  
  # convert data ------------------------------------------------------------
  
  if (dataset == 4) {
    clean_gz_2 <- grep(paste(c1[[dataset]], years_sitc_rev1, sep = "-", collapse = "|"), clean_gz, value = TRUE)
  } else {
    clean_gz_2 <- grep(c1[[dataset]], clean_gz, value = TRUE) 
  }
  
  try(dir.create(paste(converted_dir, c1[[dataset]], sep = "/")))
  
  converted_gz_2 <- clean_gz_2 %>% 
    str_replace(., clean_dir, converted_dir)
  
  converted_csv_2 <- str_replace(converted_gz_2, ".gz", "")
  
  if (operating_system != "Windows") {
    mclapply(seq_along(converted_gz_2), convert_codes, mc.cores = n_cores, 
             x = clean_gz_2, y = converted_csv_2, z = converted_gz_2)
  } else {
    lapply(seq_along(converted_gz_2), convert_codes, 
           x = clean_gz_2, y = converted_csv_2, z = converted_gz_2)
  }
}

convert()
