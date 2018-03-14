# Open oec-yearly-datasets.Rproj before running this function

organize <- function(compress_output = T) {

  # user parameters ---------------------------------------------------------

  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets ready to be added to the OEC"
  )
  message("\nCopyright (c) 2017, Datawheel\n")
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
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "HS rev 2012", "SITC rev 2"),
    title = "Select dataset:", 
    graphics = T
  )
  
  # input validation --------------------------------------------------------
  
  if (!dataset %in% 1:6) {
    stop("Only numeric values are valid for dataset selection.")
  }
  
  # packages ----------------------------------------------------------------
  
  if (!require("pacman")) install.packages("pacman")
  p_load(data.table, feather, dplyr, tidyr, janitor, doParallel)
  
  # multicore parameters ----------------------------------------------------

  #n_cores <- 4
  n_cores <- ceiling(detectCores() / 2)
  
  # years by classification -------------------------------------------------
  
  if (dataset < 6) { classification <- "hs" } else { classification <- "sitc" }
  if (dataset == 1) { rev <- 1992 }
  if (dataset == 2) { rev <- 1996 }
  if (dataset == 3) { rev <- 2002 }
  if (dataset == 4) { rev <- 2007 }
  if (dataset == 5) { rev <- 2012 }
  if (dataset == 6) { rev <- 2 }
  
  if (classification == "sitc" & rev == 2) { years <- 2000:2016 }
  if (classification == "hs") { years <- rev:2016 }
  
  # number of digits --------------------------------------------------------
  
  J <- ifelse(classification == "sitc", 4, 6)
  
  # input dirs --------------------------------------------------------------
  
  raw_dir <- "1-raw-dir"
  raw_zip_dir <- paste0(raw_dir, "/", classification, "-rev", rev, "/zip")
  raw_csv_dir <- paste0(raw_dir, "/", classification, "-rev", rev, "/csv")
  
  # output dirs -------------------------------------------------------------

  data_dir <- "2-processed-data"
  rev_dir <- paste0(data_dir, "/", classification, "-rev", rev)
  try(dir.create(rev_dir))
  
  # helpers -----------------------------------------------------------------

  messageline <- function() {
    message(rep("-", 60))
  }
  
  extract <- function(t) {
    if (file.exists(raw_csv_list[[t]])) {
      messageline()
      message(paste(raw_zip_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", raw_zip_list[[t]]))
      system(paste0("7z e -aos ", raw_zip_list[[t]], " -oc:", raw_csv_dir))
    }
  }
  
  fread2 <- function(x) {
    messageline()
    message("function fread2")
    message("x: ", x)
    as_tibble(fread(
      x, colClasses = list(
        character = c("Commodity Code"),
        numeric = c("Netweight (kg)", "Trade Value (US$)")
      )
    )) %>% clean_names()
  }
  
  file.remove2 <- function(x) {
    try(file.remove(x))
  }
  
  compress <- function(t) {
    if (file.exists(verified_zip_list[[t]])) {
      message("The compressed files exist. Skipping.")
    } else {
      messageline()
      system(paste("7z a", verified_zip_list[[t]], verified_feather_list[[t]]))
      file.remove2(verified_feather_list[[t]])
    }
  }
  
  # list input files --------------------------------------------------------

  raw_zip_list <-
    list.files(path = raw_zip_dir,
               pattern = "\\.zip",
               full.names = T) %>%
    grep(paste(paste0("ps-", years), collapse = "|"), ., value = TRUE)
  
  raw_csv_list <- raw_zip_list %>% gsub("zip", "csv", .)
  
  # list output files -------------------------------------------------------

  verified_feather_list <- paste0(rev_dir, "/", classification, "-rev", rev, "-", years, ".feather")
  
  verified_zip_list <- verified_feather_list %>%
    gsub("feather", "zip", .)
  
  # uncompress input --------------------------------------------------------

  mclapply(1:length(raw_csv_list), extract, mc.cores = n_cores)
  
  # create tidy datasets ----------------------------------------------------

  messageline()
  message("Rearranging files. Please wait...")
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = 1:length(raw_csv_list)) %dopar% {
    if (!file.exists(verified_feather_list[[t]])) {
      messageline()
      message(paste("Dividing year", years[[t]]))
      
      raw_csv <- fread2(raw_csv_list[[t]]) %>% 
        rename(trade_value_usd = trade_value_us) %>% 
        select(trade_flow, reporter_iso, partner_iso, aggregate_level, commodity_code, trade_value_usd, netweight_kg) %>% 
        filter(
          aggregate_level %in% J
        ) %>% 
        filter(
          trade_flow == "Export" | trade_flow == "Import"
        ) %>%
        filter(
          !is.na(reporter_iso),
          !is.na(partner_iso),
          reporter_iso != "", 
          partner_iso != "",
          reporter_iso != " ", 
          partner_iso != " "
        ) %>%
        mutate(
          reporter_iso = tolower(reporter_iso),
          partner_iso = tolower(partner_iso)
        )
      
      usd_spread <- raw_csv %>% 
        select(-c(aggregate_level, netweight_kg)) %>% 
        spread(trade_flow, trade_value_usd) %>% 
        rename(export_usd = Export, import_usd = Import) %>% 
        mutate(year = years[[t]]) %>%
        select(year, everything())
      
      kg_spread <- raw_csv %>% 
        select(-c(aggregate_level, trade_value_usd)) %>% 
        spread(trade_flow, netweight_kg) %>% 
        rename(export_kg = Export, import_kg = Import)
      
      rm(raw_csv)
      
      verified_feather <- usd_spread %>% 
        left_join(kg_spread) %>%
        filter(reporter_iso != "sxm", partner_iso != "sxm", # San Marino is "smr" and not "sxm"
               reporter_iso != "wld", partner_iso != "wld") %>% # the World (wld) is not needed as the OEC aggregates and computes total trade
        unite(pairs,
              reporter_iso,
              partner_iso,
              commodity_code,
              remove = FALSE)
      
      rm(usd_spread, kg_spread)
      
      replacements <- verified_feather %>%
        select(reporter_iso, partner_iso, commodity_code, export_usd, import_usd, export_kg, import_kg) %>%
        unite(pairs, partner_iso, reporter_iso, commodity_code) %>%
        rename(import_usd_rep = export_usd,
               export_usd_rep = import_usd,
               import_kg_rep = export_kg,
               export_kg_rep = import_kg)
      
      verified_feather <- verified_feather %>%
        left_join(replacements, by = "pairs") %>% 
        mutate(
          export_usd = if_else(is.na(export_usd) | export_usd == 0, export_usd_rep, export_usd),
          import_usd = if_else(is.na(import_usd) | import_usd == 0, import_usd_rep, import_usd),
          export_kg = if_else(is.na(export_kg) | export_kg == 0, export_kg_rep, export_kg),
          import_kg = if_else(is.na(import_kg) | import_kg == 0, import_kg_rep, import_kg),
          marker = ifelse(export_usd == export_usd_rep & import_usd == import_usd_rep, 3,
                          ifelse(export_usd == export_usd_rep & import_usd != import_usd_rep, 2,
                                 ifelse(export_usd != export_usd_rep & import_usd == import_usd_rep, 1, NA)))
        ) %>%
        select(-c(export_usd_rep, import_usd_rep, export_kg_rep, import_kg_rep, pairs))

      rm(replacements)
      
      write_feather(verified_feather, verified_feather_list[[t]])
      
      rm(verified_feather)
    } else {
      messageline()
      message(paste("Skipping year", years[[t]], "Files exist."))
    }
    rm(check_files)
  }
  
  stopCluster(cl)
  rm(cl)
  
  # compress output ---------------------------------------------------------
  
  if(compress_output == T) {
    mclapply(1:length(verified_zip_list), compress, mc.cores = n_cores)
  }
  
  unlink(raw_csv_dir, recursive = T)
  rm(raw_zip_list, raw_csv_list, verified_feather_list, verified_zip_list)
  
}
