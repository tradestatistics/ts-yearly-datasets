convert <- function(n_cores = 4) {
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
  
  # user parameters ---------------------------------------------------------

  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets"
  )
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

  # input dir ---------------------------------------------------------------
  
  clean_dir <- "02-clean-data-comtrade"
  
  # output dir --------------------------------------------------------------
  
  converted_dir <- "03-converted-data-comtrade"
  try(dir.create(converted_dir))
  
  # input files -------------------------------------------------------------
  
  clean_files <- list.files(clean_dir, pattern = "gz", recursive = T, full.names = T)
  
  c1 <- c("hs-rev1992", "hs-rev1996", "hs-rev2002", "sitc-rev1", "sitc-rev2")
  
  if (dataset == 4) {
    years <- 1962:1991
    clean_files_2 <- grep(paste(c1[[dataset]], years, sep = "-", collapse = "|"), clean_files, value = TRUE)
  } else {
    clean_files_2 <- grep(c1[[dataset]], clean_files, value = TRUE) 
  }
  
  # convert data ------------------------------------------------------------

  try(dir.create(paste(converted_dir, c1[[dataset]], sep = "/")))
  
  converted_gz_files <- clean_files_2 %>% 
    str_replace(., clean_dir, converted_dir)
  
  converted_csv_files <- str_replace(converted_gz_files, ".gz", "")
  
  c2 <- c("hs92", "hs96", "hs02", "sitc1", "sitc2")
  
  convert_codes <- function(t, x, y, z) {
    equivalent_codes <- product_correspondence %>% 
      select(!!sym(c2[[dataset]]), hs07) %>% 
      filter(
        !(!!sym(c2[[dataset]]) %in% c("NULL")),
        !(hs07 %in% c("NULL"))
      ) %>% 
      mutate_if(is.character, str_sub, start = 1, end = 4) %>% 
      distinct(!!sym(c2[[dataset]]), hs07)
    
    if (!file.exists(z[[t]])) {
      data <- fread3(x[[t]]) %>%
        left_join(equivalent_codes, by = c("commodity_code" = c2[[dataset]])) %>%
        distinct(reporter_iso, partner_iso, commodity_code, .keep_all = TRUE) %>% 
        mutate(
          hs07 = ifelse(is.na(hs07), 9999, hs07),
          commodity_code = hs07
        ) %>%
        select(-hs07) %>% 
        group_by(year, reporter_iso, partner_iso, commodity_code) %>% 
        summarise(trade_value_usd = sum(trade_value_usd, na.rm = TRUE))
      
      fwrite(data, y[[t]])
      compress_gz(y[[t]])
    }
  }
  
  if (operating_system != "Windows") {
    mclapply(seq_along(converted_gz_files), convert_codes, mc.cores = n_cores, 
             x = clean_files_2, y = converted_csv_files, z = converted_gz_files)
  } else {
    lapply(seq_along(converted_gz_files), convert_codes, 
           x = clean_files_2, y = converted_csv_files, z = converted_gz_files)
  }
}

convert()
