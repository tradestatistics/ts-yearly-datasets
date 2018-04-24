# Open oec-yearly-data.Rproj before running this function

tables <- function() {
  # user parameters ---------------------------------------------------------
  
  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets ready to be added to the OEC"
  )
  message("\nCopyright (c) 2017, Mauricio Pacha Vargas\n")
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
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 2"),
    title = "Select dataset:",
    graphics = T
  )
  
  J <- menu(
    c("four digits depth", "six digits depth"),
    title = "Select digits:",
    graphics = T
  )
  
  J <- ifelse(J == 1, 4, 6)
  
  # input validation --------------------------------------------------------
  
  if (dataset == 5 & J == 6) {
    message("SITC has five digits depth but the OEC uses four. Switching to four digits.")
    J <- 4
  }
  
  # packages ----------------------------------------------------------------
  
  if (!require("pacman")) install.packages("pacman")
  p_load(Matrix, data.table, feather, dplyr, tidyr, stringr, doParallel)
  
  # multicore parameters ----------------------------------------------------
  
  #n_cores <- 4
  n_cores <- ceiling(detectCores() / 2) - 1
  
  # years by classification -------------------------------------------------
  
  if (dataset < 5) { classification <- "hs" } else { classification <- "sitc" }
  if (dataset == 1) { rev <- 1992 }
  if (dataset == 2) { rev <- 1996 }
  if (dataset == 3) { rev <- 2002 }
  if (dataset == 4) { rev <- 2007 }
  if (dataset == 5) { rev <- 2 }
  
  if (classification == "sitc" & rev == 2) { years <- 1990:2016 }
  if (classification == "hs") { years <- rev:2016 }
  
  if (classification == "sitc" & rev == 2) {
    years_missing_t_minus_1 <- 1990; years_missing_t_minus_5 <- 1991:1994; years_full <- 1995:2016
  }
  if (classification == "hs" & rev == 1992) {
    years_missing_t_minus_1 <- 1992; years_missing_t_minus_5 <- 1993:1996; years_full <- 1997:2016
  }
  if (classification == "hs" & rev == 1996) {
    years_missing_t_minus_1 <- 1996; years_missing_t_minus_5 <- 1997:2000; years_full <- 2001:2016
  }
  if (classification == "hs" & rev == 2002) {
    years_missing_t_minus_1 <- 2002; years_missing_t_minus_5 <- 2003:2006; years_full <- 2007:2016
  }
  if (classification == "hs" & rev == 2007) {
    years_missing_t_minus_1 <- 2007; years_missing_t_minus_5 <- 2008:2011; years_full <- 2012:2016
  }
  
  # input data --------------------------------------------------------------
  
  country_names <- as_tibble(fread("../oec-observatory-codes/1-country-data/country-names.tsv")) %>%
    select(-name)
  
  if (dataset == 1) {
    product_names <- as_tibble(fread("../oec-observatory-codes/2-product-data/hs-rev1992-product-names.tsv")) %>%
      select(-name) %>%
      rename(
        commodity_code = hs92,
        prod_id = id
      )
  }
  
  if (dataset == 2) {
    product_names <- as_tibble(fread("../oec-observatory-codes/2-product-data/hs-rev1996-product-names.tsv")) %>%
      select(-name) %>%
      rename(
        commodity_code = hs96,
        prod_id = id
      )
  }
  
  if (dataset == 3) {
    product_names <- as_tibble(fread("../oec-observatory-codes/2-product-data/hs-rev2002-product-names.tsv")) %>%
      select(-name) %>%
      rename(
        commodity_code = hs02,
        prod_id = id
      )
  }
  
  if (dataset == 4) {
    product_names <- as_tibble(fread("../oec-observatory-codes/2-product-data/hs-rev2007-product-names.tsv")) %>%
      select(-name) %>%
      rename(
        commodity_code = hs07,
        prod_id = id
      )
  }
  
  if (dataset == 5) {
    product_names <- as_tibble(fread("../oec-observatory-codes/2-product-data/sitc-rev2-product-names.tsv")) %>%
      select(-name) %>%
      rename(
        commodity_code = sitc,
        prod_id = id
      )
  }
  
  pci <- as_tibble(fread(
    sprintf("1-3-measures/%s-rev%s-%s/pci-joined-ranking.csv", classification, rev, J),
    colClasses = c("commodity_code" = "character")
  ))
  
  # input dirs --------------------------------------------------------------
  
  clean_dir <- "1-2-clean-data"
  clean_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, rev)
  
  measures_dir <- "1-3-measures"
  rca_exports_dir <- sprintf("%s/%s-rev%s-%s/smooth-rca-exports", measures_dir, classification, rev, J)
  rca_imports_dir <- sprintf("%s/%s-rev%s-%s/smooth-rca-imports", measures_dir, classification, rev, J)
  
  # output dirs -------------------------------------------------------------
  
  tables_dir <- "1-4-tables"
  tables_dir <- sprintf("%s/%s-rev%s-%s", tables_dir, classification, rev, J)
  try(dir.create(tables_dir))
  
  rca_exp_dir <- paste0(tables_dir, "/rca-exp")
  try(dir.create(rca_exp_dir))
  
  rca_imp_dir <- paste0(tables_dir, "/rca-imp")
  try(dir.create(rca_imp_dir))
  
  yodp_dir <- paste0(tables_dir, "/1-yodp")
  try(dir.create(yodp_dir))
  
  yod_dir <- paste0(tables_dir, "/2-yod")
  try(dir.create(yod_dir))
  
  yop_dir <- paste0(tables_dir, "/3-yop")
  try(dir.create(yop_dir))
  
  ydp_dir <- paste0(tables_dir, "/4-ydp")
  try(dir.create(ydp_dir))
  
  yo_dir <- paste0(tables_dir, "/5-yo")
  try(dir.create(yo_dir))
  
  yd_dir <- paste0(tables_dir, "/6-yd")
  try(dir.create(yd_dir))
  
  yp_dir <- paste0(tables_dir, "/7-yp")
  try(dir.create(yp_dir))
  
  # helpers -----------------------------------------------------------------
  
  messageline <- function() {
    message(rep("-", 60))
  }
  
  extract <- function(t) {
    if (file.exists(clean_feather_list[[t]])) {
      messageline()
      message(paste(clean_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", clean_zip_list[[t]]))
      system(paste0("7z e -aos ", clean_zip_list[[t]], " -oc:", tables_dir))
    }
  }
  
  extract_rca_exp <- function(t) {
    if (file.exists(rca_exp_feather_list[[t]])) {
      messageline()
      message(paste(rca_exp_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", rca_exp_zip_list[[t]]))
      system(paste0("7z e -aos ", rca_exp_zip_list[[t]], " -oc:", rca_exp_dir))
    }
  }
  
  extract_rca_imp <- function(t) {
    if (file.exists(rca_imp_feather_list[[t]])) {
      messageline()
      message(paste(rca_imp_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", rca_imp_zip_list[[t]]))
      system(paste0("7z e -aos ", rca_imp_zip_list[[t]], " -oc:", rca_imp_dir))
    }
  }
  
  file.remove2 <- function(t) {
    try(file.remove(t))
  }
  
  compress <- function(t) {
    if (file.exists(tables_zip_list[[t]])) {
      message("The compressed files exist. Skipping.")
    } else {
      messageline()
      system(paste("7z a", tables_zip_list[[t]], tables_csv_list[[t]]))
      file.remove2(tables_csv_list[[t]])
    }
  }
  
  # list input files --------------------------------------------------------
  
  clean_zip_list <-
    list.files(path = clean_dir,
               pattern = "\\.zip",
               full.names = T) %>%
    grep(paste(years, collapse = "|"), ., value = TRUE)
  
  rca_exp_zip_list <- sprintf("%s/smooth-rca-exports-%s.zip", rca_exports_dir, years)
  
  rca_imp_zip_list <- sprintf("%s/smooth-rca-imports-%s.zip", rca_imports_dir, years)
  
  # list output files -------------------------------------------------------
  
  clean_feather_list <- clean_zip_list %>%
    gsub("zip", "feather", .) %>%
    gsub(clean_dir, tables_dir, .)
  
  rca_exp_feather_list <- rca_exp_zip_list %>%
    gsub("zip", "feather", .) %>%
    gsub(rca_exports_dir, rca_exp_dir, .)
  
  rca_imp_feather_list <- rca_imp_zip_list %>%
    gsub("zip", "feather", .) %>%
    gsub(rca_imports_dir, rca_imp_dir, .)
  
  yodp_csv_list <- sprintf("%s/yodp-%s-rev%s-%s-%s.csv", yodp_dir, classification, rev, J, years)
  yodp_zip_list <- yodp_csv_list %>% gsub("csv", "zip", .)
  
  yod_csv_list <- sprintf("%s/yod-%s-rev%s-%s-%s.csv", yod_dir, classification, rev, J, years)
  yod_zip_list <- yod_csv_list %>% gsub("csv", "zip", .)
  
  yop_csv_list <- sprintf("%s/yop-%s-rev%s-%s-%s.csv", yop_dir, classification, rev, J, years)
  yop_zip_list <- yop_csv_list %>% gsub("csv", "zip", .)
  
  ydp_csv_list <- sprintf("%s/ydp-%s-rev%s-%s-%s.csv", ydp_dir, classification, rev, J, years)
  ydp_zip_list <- ydp_csv_list %>% gsub("csv", "zip", .)
  
  yo_csv_list <- sprintf("%s/yo-%s-rev%s-%s-%s.csv", yo_dir, classification, rev, J, years)
  yo_zip_list <- yo_csv_list %>% gsub("csv", "zip", .)
  
  yd_csv_list <- sprintf("%s/yd-%s-rev%s-%s-%s.csv", yd_dir, classification, rev, J, years)
  yd_zip_list <- yd_csv_list %>% gsub("csv", "zip", .)
  
  yp_csv_list <- sprintf("%s/yp-%s-rev%s-%s-%s.csv", yp_dir, classification, rev, J, years)
  yp_zip_list <- yp_csv_list %>% gsub("csv", "zip", .)
  
  # uncompress input --------------------------------------------------------
  
  mclapply(1:length(clean_feather_list), extract, mc.cores = n_cores)
  
  mclapply(1:length(rca_exp_feather_list), extract_rca_exp, mc.cores = n_cores)
  
  mclapply(1:length(rca_imp_feather_list), extract_rca_imp, mc.cores = n_cores)
  
  # tables 1 ---------------------------------------------------------------
  
  for (t in match(years_missing_t_minus_1, years)) {
    if(file.exists(yodp_csv_list[[t]]) |
       file.exists(yodp_zip_list[[t]])) {
      messageline()
      message(paste("YODP table for the year", years[t], "exists. Skipping."))
    } else {
      messageline()
      message(paste("Creating YODP table for the year", years[t]))
      
      # yodp 1 ------------------------------------------------------------------
      
      yodp_t1 <- read_feather(clean_feather_list[[t]]) %>%
        rename(
          export_val = export_usd,
          import_val = import_usd
        ) %>%
        mutate(commodity_code = str_sub(commodity_code, 1, J)) %>%
        group_by(year, reporter_iso, partner_iso, commodity_code) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
        left_join(product_names) %>%
        rename(
          origin_id = id.x,
          dest_id = id.y
        ) %>%
        select(year, origin_id, dest_id, prod_id, prod_id_len, export_val, import_val)
      
      yodp_t1 <- yodp_t1 %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        ) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val)
        )
      
      fwrite(yodp_t1, yodp_csv_list[[t]])
      
      # yod 1 ------------------------------------------------------------------
      
      yod_t1 <- yodp_t1 %>%
        select(year, origin_id, dest_id, export_val, import_val) %>%
        group_by(year, origin_id, dest_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        )
      
      fwrite(yod_t1, yod_csv_list[[t]])
      
      rm(yod_t1)
      
      # yop 1 ------------------------------------------------------------------
      
      rca_exp <- read_feather(rca_exp_feather_list[[t]]) %>%
        left_join(product_names) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        select(-c(year, commodity_code, reporter_iso)) %>%
        rename(
          origin_id = id,
          export_rca = rca_export_smooth
        )
      
      rca_imp <- read_feather(rca_imp_feather_list[[t]]) %>%
        left_join(product_names) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        select(-c(year, commodity_code, reporter_iso)) %>%
        rename(
          origin_id = id,
          import_rca = rca_import_smooth
        )
      
      yop_t1 <- yodp_t1 %>%
        select(year, origin_id, prod_id, export_val, import_val) %>%
        group_by(year, origin_id, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        select(year, origin_id, prod_id, prod_id_len, export_val, import_val) %>%
        left_join(rca_exp) %>%
        left_join(rca_imp) %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        )
      
      fwrite(yop_t1, yop_csv_list[[t]])
      
      rm(yop_t1)
      
      # ydp 1 ------------------------------------------------------------------
      
      ydp_t1 <- yodp_t1 %>%
        select(year, dest_id, prod_id, export_val, import_val) %>%
        group_by(year, dest_id, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        select(year, dest_id, prod_id, prod_id_len, export_val, import_val) %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        )
      
      fwrite(ydp_t1, ydp_csv_list[[t]])
      
      rm(ydp_t1)
      
      # yo 1 -------------------------------------------------------------------
      
      max_exp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(export_val = sum(export_val, na.rm = T)) %>%
        group_by(origin_id) %>%
        slice(which.max(export_val)) %>%
        rename(
          top_export_id = prod_id,
          top_export = export_val
        )
      
      max_imp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(import_val = sum(import_val, na.rm = T)) %>%
        group_by(origin_id) %>%
        slice(which.max(import_val)) %>%
        rename(
          top_import_id = prod_id,
          top_import = import_val
        )
      
      yo_t1 <- yodp_t1 %>%
        select(year, origin_id, export_val, import_val) %>%
        group_by(year, origin_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        left_join(max_exp, by = "origin_id") %>%
        left_join(max_imp, by = "origin_id") %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        )
      
      fwrite(yo_t1, yo_csv_list[[t]])
      
      rm(yo_t1, max_exp, max_imp)
      
      # yd 1 -------------------------------------------------------------------
      
      yd_t1 <- yodp_t1 %>%
        select(year, dest_id, export_val, import_val) %>%
        group_by(year, dest_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        )
      
      fwrite(yd_t1, yd_csv_list[[t]])
      
      rm(yd_t1)
      
      # yp 1 -------------------------------------------------------------------
      
      pci_t1 <- pci %>%
        filter(year == years[[t]]) %>%
        left_join(product_names) %>%
        select(-c(year, commodity_code))
      
      max_exp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(export_val = sum(export_val, na.rm = T)) %>%
        group_by(prod_id) %>%
        slice(which.max(export_val)) %>%
        rename(top_exporter_id = origin_id) %>%
        select(-export_val)
      
      max_imp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(import_val = sum(import_val, na.rm = T)) %>%
        group_by(prod_id) %>%
        slice(which.max(import_val)) %>%
        rename(top_importer_id = origin_id) %>%
        select(-import_val)
      
      yp_t1 <- yodp_t1 %>%
        select(year, prod_id, export_val, import_val) %>%
        group_by(year, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        select(year, prod_id, prod_id_len, export_val, import_val) %>%
        left_join(pci_t1, by = "prod_id") %>%
        mutate(pci_rank_delta = NA) %>%
        left_join(max_exp, by = "prod_id") %>%
        left_join(max_imp, by = "prod_id") %>%
        mutate(
          export_val_growth_val = NA,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = NA,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = NA,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = NA,
          import_val_growth_pct_5 = NA
        )
      
      fwrite(yp_t1, yp_csv_list[[t]])
      
      rm(yodp_t1, yp_t1, pci_t1, max_exp, max_imp)
    }
  }
  
  # tables 2 ---------------------------------------------------------------
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = match(years_missing_t_minus_5, years)) %dopar% {
    if(file.exists(yodp_csv_list[[t]]) |
       file.exists(yodp_zip_list[[t]])) {
      messageline()
      message(paste("YODP table for the year", years[t], "exists. Skipping."))
    } else {
      messageline()
      message(paste("Creating YODP table for the year", years[t]))
      
      # yodp 2 ------------------------------------------------------------------
      
      yodp_t1 <- read_feather(clean_feather_list[[t]]) %>%
        rename(
          export_val = export_usd,
          import_val = import_usd
        ) %>%
        mutate(commodity_code = str_sub(commodity_code, 1, J)) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
        left_join(product_names) %>%
        rename(
          origin_id = id.x,
          dest_id = id.y
        ) %>%
        unite(pairs, origin_id, dest_id, prod_id) %>%
        group_by(year, pairs) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        select(year, pairs, prod_id_len, export_val, import_val)
      
      yodp_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
        rename(
          export_val = export_usd,
          import_val = import_usd
        ) %>%
        mutate(commodity_code = str_sub(commodity_code, 1, J)) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
        left_join(product_names) %>%
        rename(
          origin_id = id.x,
          dest_id = id.y
        ) %>%
        unite(pairs, origin_id, dest_id, prod_id) %>%
        group_by(pairs) %>%
        summarise(
          export_val_t2 = sum(export_val, na.rm = T),
          import_val_t2 = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        select(pairs, export_val_t2, import_val_t2)
      
      yodp_t1 <- yodp_t1 %>%
        left_join(yodp_t2) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        separate(pairs, c("origin_id", "dest_id", "prod_id"))
      
      fwrite(yodp_t1 %>% select(-c(export_val_t2, import_val_t2)), yodp_csv_list[[t]])
      
      rm(yodp_t2)
      
      # yod 2 ------------------------------------------------------------------
      
      yod_t1 <- yodp_t1 %>%
        select(year, origin_id, dest_id, export_val, import_val, export_val_t2, import_val_t2) %>%
        group_by(year, origin_id, dest_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        select(-c(export_val_t2, import_val_t2))
      
      fwrite(yod_t1, yod_csv_list[[t]])
      
      rm(yod_t1)
      
      # yop 2 ------------------------------------------------------------------
      
      rca_exp <- read_feather(rca_exp_feather_list[[t]]) %>%
        left_join(product_names) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        select(-c(year, commodity_code, reporter_iso)) %>%
        rename(
          origin_id = id,
          export_rca = rca_export_smooth
        )
      
      rca_imp <- read_feather(rca_imp_feather_list[[t]]) %>%
        left_join(product_names) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        select(-c(year, commodity_code, reporter_iso)) %>%
        rename(
          origin_id = id,
          import_rca = rca_import_smooth
        )
      
      yop_t1 <- yodp_t1 %>%
        select(year, origin_id, prod_id, export_val, import_val, export_val_t2, import_val_t2) %>%
        group_by(year, origin_id, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        left_join(rca_exp) %>%
        left_join(rca_imp) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        select(year, origin_id, prod_id, prod_id_len, everything()) %>%
        select(-c(export_val_t2, import_val_t2))
      
      fwrite(yop_t1, yop_csv_list[[t]])
      
      rm(yop_t1)
      
      # ydp 2 ------------------------------------------------------------------
      
      ydp_t1 <- yodp_t1 %>%
        select(year, dest_id, prod_id, export_val, import_val, export_val_t2, import_val_t2) %>%
        group_by(year, dest_id, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        select(year, dest_id, prod_id, prod_id_len, everything()) %>%
        select(-c(export_val_t2, import_val_t2))
      
      fwrite(ydp_t1, ydp_csv_list[[t]])
      
      rm(ydp_t1)
      
      # yo 2 -------------------------------------------------------------------
      
      max_exp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(export_val = sum(export_val, na.rm = T)) %>%
        group_by(origin_id) %>%
        slice(which.max(export_val)) %>%
        rename(
          top_export_id = prod_id,
          top_export = export_val
        )
      
      max_imp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(import_val = sum(import_val, na.rm = T)) %>%
        group_by(origin_id) %>%
        slice(which.max(import_val)) %>%
        rename(
          top_import_id = prod_id,
          top_import = import_val
        )
      
      yo_t1 <- yodp_t1 %>%
        select(year, origin_id, export_val, import_val, export_val_t2, import_val_t2) %>%
        group_by(year, origin_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T)
        ) %>%
        ungroup() %>%
        left_join(max_exp, by = "origin_id") %>%
        left_join(max_imp, by = "origin_id") %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        select(-c(export_val_t2, import_val_t2))
      
      fwrite(yo_t1, yo_csv_list[[t]])
      
      rm(yo_t1, max_exp, max_imp)
      
      # yd 2 -------------------------------------------------------------------
      
      yd_t1 <- yodp_t1 %>%
        select(year, dest_id, export_val, import_val, export_val_t2, import_val_t2) %>%
        group_by(year, dest_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        select(-c(export_val_t2, import_val_t2))
      
      fwrite(yd_t1, yd_csv_list[[t]])
      
      rm(yd_t1)
      
      # yp 2 -------------------------------------------------------------------
      
      pci_t1 <- pci %>%
        filter(year == years[[t]]) %>%
        left_join(product_names) %>%
        select(-c(year, commodity_code))
      
      pci_t2 <- pci %>%
        filter(year == years[[t-1]]) %>%
        left_join(product_names) %>%
        select(-c(year, commodity_code))
      
      max_exp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(export_val = sum(export_val, na.rm = T)) %>%
        group_by(prod_id) %>%
        slice(which.max(export_val)) %>%
        rename(top_exporter_id = origin_id) %>%
        select(-export_val)
      
      max_imp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(import_val = sum(import_val, na.rm = T)) %>%
        group_by(prod_id) %>%
        slice(which.max(import_val)) %>%
        rename(top_importer_id = origin_id) %>%
        select(-import_val)
      
      yp_t1 <- yodp_t1 %>%
        select(year, prod_id, export_val, import_val, export_val_t2, import_val_t2) %>%
        group_by(year, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        left_join(pci_t1, by = "prod_id") %>%
        left_join(pci_t2, by = "prod_id") %>%
        mutate(pci_rank_delta = pci_rank.x - pci_rank.y) %>%
        select(-c(pci_rank.x, pci_rank.y, pci.y)) %>%
        rename(pci = pci.x) %>%
        left_join(max_exp, by = "prod_id") %>%
        left_join(max_imp, by = "prod_id") %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = NA,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = NA,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = NA,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = NA
        ) %>%
        select(-c(export_val_t2, import_val_t2)) %>%
        select(year, prod_id, prod_id_len, everything())
      
      fwrite(yp_t1, yp_csv_list[[t]])
      
      rm(yodp_t1, yp_t1, pci_t1, max_exp, max_imp)
    }
  }
  
  stopCluster(cl)
  rm(cl)
  
  # tables 3 ---------------------------------------------------------------
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = match(years_full, years)) %dopar% {
    if(file.exists(yodp_csv_list[[t]]) |
       file.exists(yodp_zip_list[[t]])) {
      messageline()
      message(paste("YODP table for the year", years[t], "exists. Skipping."))
    } else {
      messageline()
      message(paste("Creating YODP table for the year", years[t]))
      
      # yodp 3 ------------------------------------------------------------------
      
      yodp_t1 <- read_feather(clean_feather_list[[t]]) %>%
        rename(
          export_val = export_usd,
          import_val = import_usd
        ) %>%
        mutate(commodity_code = str_sub(commodity_code, 1, J)) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
        left_join(product_names) %>%
        rename(
          origin_id = id.x,
          dest_id = id.y
        ) %>%
        unite(pairs, origin_id, dest_id, prod_id) %>%
        group_by(year, pairs) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        select(year, pairs, prod_id_len, export_val, import_val)
      
      yodp_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
        rename(
          export_val = export_usd,
          import_val = import_usd
        ) %>%
        mutate(commodity_code = str_sub(commodity_code, 1, J)) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
        left_join(product_names) %>%
        rename(
          origin_id = id.x,
          dest_id = id.y
        ) %>%
        unite(pairs, origin_id, dest_id, prod_id) %>%
        group_by(pairs) %>%
        summarise(
          export_val_t2 = sum(export_val, na.rm = T),
          import_val_t2 = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        select(pairs, export_val_t2, import_val_t2)
      
      yodp_t3 <- read_feather(clean_feather_list[[t-5]]) %>%
        rename(
          export_val = export_usd,
          import_val = import_usd
        ) %>%
        mutate(commodity_code = str_sub(commodity_code, 1, J)) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
        left_join(product_names) %>%
        rename(
          origin_id = id.x,
          dest_id = id.y
        ) %>%
        unite(pairs, origin_id, dest_id, prod_id) %>%
        group_by(pairs) %>%
        summarise(
          export_val_t3 = sum(export_val, na.rm = T),
          import_val_t3 = sum(import_val, na.rm = T)
        ) %>%
        ungroup() %>%
        select(pairs, export_val_t3, import_val_t3)
      
      yodp_t1 <- yodp_t1 %>%
        left_join(yodp_t2) %>%
        left_join(yodp_t3) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        separate(pairs, c("origin_id", "dest_id", "prod_id"))
      
      fwrite(yodp_t1 %>% select(-c(matches("export_val_t"), matches("import_val_t"))), yodp_csv_list[[t]])
      
      rm(yodp_t2)
      
      # yod 3 ------------------------------------------------------------------
      
      yod_t1 <- yodp_t1 %>%
        select(year, origin_id, dest_id, matches("export_val"), matches("import_val")) %>%
        group_by(year, origin_id, dest_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yod_t1, yod_csv_list[[t]])
      
      rm(yod_t1)
      
      # yop 3 ------------------------------------------------------------------
      
      rca_exp <- read_feather(rca_exp_feather_list[[t]]) %>%
        left_join(product_names) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        select(-c(year, commodity_code, reporter_iso)) %>%
        rename(
          origin_id = id,
          export_rca = rca_export_smooth
        )
      
      rca_imp <- read_feather(rca_imp_feather_list[[t]]) %>%
        left_join(product_names) %>%
        left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
        select(-c(year, commodity_code, reporter_iso)) %>%
        rename(
          origin_id = id,
          import_rca = rca_import_smooth
        )
      
      yop_t1 <- yodp_t1 %>%
        select(year, origin_id, prod_id, matches("export_val"), matches("import_val")) %>%
        group_by(year, origin_id, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        left_join(rca_exp) %>%
        left_join(rca_imp) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        select(year, origin_id, prod_id, prod_id_len, everything()) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yop_t1, yop_csv_list[[t]])
      
      rm(yop_t1)
      
      # ydp 3 ------------------------------------------------------------------
      
      ydp_t1 <- yodp_t1 %>%
        select(year, dest_id, prod_id, matches("export_val"), matches("import_val")) %>%
        group_by(year, dest_id, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        select(year, dest_id, prod_id, prod_id_len, everything()) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(ydp_t1, ydp_csv_list[[t]])
      
      rm(ydp_t1)
      
      # yo 3 -------------------------------------------------------------------
      
      max_exp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(export_val = sum(export_val, na.rm = T)) %>%
        group_by(origin_id) %>%
        slice(which.max(export_val)) %>%
        rename(
          top_export_id = prod_id,
          top_export = export_val
        )
      
      max_imp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(import_val = sum(import_val, na.rm = T)) %>%
        group_by(origin_id) %>%
        slice(which.max(import_val)) %>%
        rename(
          top_import_id = prod_id,
          top_import = import_val
        )
      
      yo_t1 <- yodp_t1 %>%
        select(year, origin_id, matches("export_val"), matches("import_val")) %>%
        group_by(year, origin_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() %>%
        left_join(max_exp, by = "origin_id") %>%
        left_join(max_imp, by = "origin_id") %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yo_t1, yo_csv_list[[t]])
      
      rm(yo_t1, max_exp, max_imp)
      
      # yd 3 -------------------------------------------------------------------
      
      yd_t1 <- yodp_t1 %>%
        select(year, dest_id, matches("export_val"), matches("import_val")) %>%
        group_by(year, dest_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yd_t1, yd_csv_list[[t]])
      
      rm(yd_t1)
      
      # yp 3 -------------------------------------------------------------------
      
      pci_t1 <- pci %>%
        filter(year == years[[t]]) %>%
        left_join(product_names) %>%
        select(-c(year, commodity_code))
      
      pci_t2 <- pci %>%
        filter(year == years[[t-1]]) %>%
        left_join(product_names) %>%
        select(-c(year, commodity_code))
      
      max_exp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(export_val = sum(export_val, na.rm = T)) %>%
        group_by(prod_id) %>%
        slice(which.max(export_val)) %>%
        rename(top_exporter_id = origin_id) %>%
        select(-export_val)
      
      max_imp <- yodp_t1 %>%
        group_by(origin_id, prod_id) %>%
        summarise(import_val = sum(import_val, na.rm = T)) %>%
        group_by(prod_id) %>%
        slice(which.max(import_val)) %>%
        rename(top_importer_id = origin_id) %>%
        select(-import_val)
      
      yp_t1 <- yodp_t1 %>%
        select(year, prod_id, matches("export_val"), matches("import_val")) %>%
        group_by(year, prod_id) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(prod_id_len = J) %>%
        left_join(pci_t1, by = "prod_id") %>%
        left_join(pci_t2, by = "prod_id") %>%
        mutate(pci_rank_delta = pci_rank.x - pci_rank.y) %>%
        select(-c(pci_rank.x, pci_rank.y, pci.y)) %>%
        rename(pci = pci.x) %>%
        left_join(max_exp, by = "prod_id") %>%
        left_join(max_imp, by = "prod_id") %>%
        mutate(
          export_val = if_else(export_val == 0, as.numeric(NA), export_val),
          import_val = if_else(import_val == 0, as.numeric(NA), import_val),
          export_val_t2 = if_else(export_val_t2 == 0, as.numeric(NA), export_val_t2),
          import_val_t2 = if_else(import_val_t2 == 0, as.numeric(NA), import_val_t2),
          export_val_t3 = if_else(export_val_t3 == 0, as.numeric(NA), export_val_t3),
          import_val_t3 = if_else(import_val_t3 == 0, as.numeric(NA), import_val_t3)
        ) %>%
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3,
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3,
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3,
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        select(-c(matches("export_val_t"), matches("import_val_t"))) %>%
        select(year, prod_id, prod_id_len, everything())
      
      fwrite(yp_t1, yp_csv_list[[t]])
      
      rm(yodp_t1, yp_t1, pci_t1, max_exp, max_imp)
    }
  }
  
  stopCluster(cl)
  rm(cl)
  
  # compress output ---------------------------------------------------------
  
  tables_csv_list <- list.files(tables_dir, pattern = "csv", full.names = T, recursive = T)
  tables_zip_list <- gsub("csv", "zip", tables_csv_list)
  
  mclapply(1:length(tables_csv_list), compress, mc.cores = n_cores)
  
  lapply(clean_feather_list, file.remove2)
}

tables()
