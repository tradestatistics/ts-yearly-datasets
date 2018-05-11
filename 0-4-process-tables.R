# Open oec-yearly-data.Rproj before running this function

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
p_load(Matrix, data.table, feather, dplyr, tidyr, stringr, doParallel)

tables <- function() {
  # user parameters ---------------------------------------------------------
  
  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets ready to be added to the OEC"
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
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 2"),
    title = "Select dataset:",
    graphics = T
  )
  
  # multicore parameters ----------------------------------------------------
  
  #n_cores <- 4
  n_cores <- floor(detectCores() / 2)
  
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
  
  pci4 <- as_tibble(fread(
    sprintf("1-3-measures/%s-rev%s-%s/pci-joined-ranking.csv", classification, rev, 4),
    colClasses = c("commodity_code" = "character")
  ))
  
  if (classification == "hs") {
    pci6 <- as_tibble(fread(
      sprintf("1-3-measures/%s-rev%s-%s/pci-joined-ranking.csv", classification, rev, 6),
      colClasses = c("commodity_code" = "character")
    ))
    
    pci <- bind_rows(pci4, pci6)
    rm(pci4, pci6)
  } else {
    pci <- pci4
    rm(pci4)
  }

  # input dirs --------------------------------------------------------------
  
  clean_dir <- "1-2-clean-data"
  clean_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, rev)
  
  measures_dir <- "1-3-measures"
  
  rca_exports_4_dir <- sprintf("%s/%s-rev%s-%s/smooth-rca-exports", measures_dir, classification, rev, 4)
  rca_imports_4_dir <- sprintf("%s/%s-rev%s-%s/smooth-rca-imports", measures_dir, classification, rev, 4)
  
  if (classification == "hs") {
    rca_exports_6_dir <- sprintf("%s/%s-rev%s-%s/smooth-rca-exports", measures_dir, classification, rev, 6)
    rca_imports_6_dir <- sprintf("%s/%s-rev%s-%s/smooth-rca-imports", measures_dir, classification, rev, 6)
  }
  
  # output dirs -------------------------------------------------------------
  
  tables_dir <- "1-4-tables"
  
  tables_dir <- sprintf("%s/%s-rev%s", tables_dir, classification, rev)
  try(dir.create(tables_dir))
  
  rca_exp_4_dir <- paste0(tables_dir, "/rca-exp-4")
  try(dir.create(rca_exp_4_dir))
  
  rca_imp_4_dir <- paste0(tables_dir, "/rca-imp-4")
  try(dir.create(rca_imp_4_dir))
  
  if (classification == "hs") {
    rca_exp_6_dir <- paste0(tables_dir, "/rca-exp-6")
    try(dir.create(rca_exp_6_dir))
    
    rca_imp_6_dir <- paste0(tables_dir, "/rca-imp-6")
    try(dir.create(rca_imp_6_dir))
  }
  
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
  
  extract_rca_exp_4 <- function(t) {
    if (file.exists(rca_exp_4_feather_list[[t]])) {
      messageline()
      message(paste(rca_exp_4_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", rca_exp_4_zip_list[[t]]))
      system(paste0("7z e -aos ", rca_exp_4_zip_list[[t]], " -oc:", rca_exp_4_dir))
    }
  }
  
  extract_rca_imp_4 <- function(t) {
    if (file.exists(rca_imp_4_feather_list[[t]])) {
      messageline()
      message(paste(rca_imp_4_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", rca_imp_4_zip_list[[t]]))
      system(paste0("7z e -aos ", rca_imp_4_zip_list[[t]], " -oc:", rca_imp_4_dir))
    }
  }
  
  extract_rca_exp_6 <- function(t) {
    if (file.exists(rca_exp_6_feather_list[[t]])) {
      messageline()
      message(paste(rca_exp_6_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", rca_exp_6_zip_list[[t]]))
      system(paste0("7z e -aos ", rca_exp_6_zip_list[[t]], " -oc:", rca_exp_6_dir))
    }
  }
  
  extract_rca_imp_6 <- function(t) {
    if (file.exists(rca_imp_6_feather_list[[t]])) {
      messageline()
      message(paste(rca_imp_6_feather_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", rca_imp_6_zip_list[[t]]))
      system(paste0("7z e -aos ", rca_imp_6_zip_list[[t]], " -oc:", rca_imp_6_dir))
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
  
  rca_exp_4_zip_list <- sprintf("%s/smooth-rca-exports-%s.zip", rca_exports_4_dir, years)
  rca_imp_4_zip_list <- sprintf("%s/smooth-rca-imports-%s.zip", rca_imports_4_dir, years)
  
  if (classification == "hs") {
    rca_exp_6_zip_list <- sprintf("%s/smooth-rca-exports-%s.zip", rca_exports_6_dir, years)
    rca_imp_6_zip_list <- sprintf("%s/smooth-rca-imports-%s.zip", rca_imports_6_dir, years)
  }
  
  # list output files -------------------------------------------------------
  
  clean_feather_list <- clean_zip_list %>%
    gsub("zip", "feather", .) %>%
    gsub(clean_dir, tables_dir, .)
  
  rca_exp_4_feather_list <- rca_exp_4_zip_list %>%
    gsub("zip", "feather", .) %>%
    gsub(rca_exports_4_dir, rca_exp_4_dir, .)
  
  rca_imp_4_feather_list <- rca_imp_4_zip_list %>%
    gsub("zip", "feather", .) %>%
    gsub(rca_imports_4_dir, rca_imp_4_dir, .)
  
  if (classification == "hs") {
    rca_exp_6_feather_list <- rca_exp_6_zip_list %>%
      gsub("zip", "feather", .) %>%
      gsub(rca_exports_6_dir, rca_exp_6_dir, .)
    
    rca_imp_6_feather_list <- rca_imp_6_zip_list %>%
      gsub("zip", "feather", .) %>%
      gsub(rca_imports_6_dir, rca_imp_6_dir, .)
  }
  
  yodp_csv_list <- sprintf("%s/yodp-%s-rev%s-%s.csv", yodp_dir, classification, rev, years)
  yodp_zip_list <- yodp_csv_list %>% gsub("csv", "zip", .)
  
  yod_csv_list <- sprintf("%s/yod-%s-rev%s-%s.csv", yod_dir, classification, rev, years)
  yod_zip_list <- yod_csv_list %>% gsub("csv", "zip", .)
  
  yop_csv_list <- sprintf("%s/yop-%s-rev%s-%s.csv", yop_dir, classification, rev, years)
  yop_zip_list <- yop_csv_list %>% gsub("csv", "zip", .)
  
  ydp_csv_list <- sprintf("%s/ydp-%s-rev%s-%s.csv", ydp_dir, classification, rev, years)
  ydp_zip_list <- ydp_csv_list %>% gsub("csv", "zip", .)
  
  yo_csv_list <- sprintf("%s/yo-%s-rev%s-%s.csv", yo_dir, classification, rev, years)
  yo_zip_list <- yo_csv_list %>% gsub("csv", "zip", .)
  
  yd_csv_list <- sprintf("%s/yd-%s-rev%s-%s.csv", yd_dir, classification, rev, years)
  yd_zip_list <- yd_csv_list %>% gsub("csv", "zip", .)
  
  yp_csv_list <- sprintf("%s/yp-%s-rev%s-%s.csv", yp_dir, classification, rev, years)
  yp_zip_list <- yp_csv_list %>% gsub("csv", "zip", .)
  
  # uncompress input --------------------------------------------------------
  
  mclapply(1:length(clean_feather_list), extract, mc.cores = n_cores)
  
  mclapply(1:length(rca_exp_4_feather_list), extract_rca_exp_4, mc.cores = n_cores)
  mclapply(1:length(rca_imp_4_feather_list), extract_rca_imp_4, mc.cores = n_cores)
  
  if (classification == "hs") {
    mclapply(1:length(rca_exp_6_feather_list), extract_rca_exp_6, mc.cores = n_cores)
    mclapply(1:length(rca_imp_6_feather_list), extract_rca_imp_6, mc.cores = n_cores)
  }
  
  # tables ------------------------------------------------------------------

  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = 1:length(years)) %dopar% {
    if(file.exists(yodp_csv_list[[t]]) |
       file.exists(yodp_zip_list[[t]])) {
      messageline()
      message(paste("YODP table for the year", years[t], "exists. Skipping."))
    } else {
      messageline()
      message(paste("Creating YODP table for the year", years[t]))
      
      # yodp ------------------------------------------------------------------
      
      if (classification == "hs") {
        yodp_4_t1 <- read_feather(clean_feather_list[[t]]) %>%
          rename(
            export_val = export_usd,
            import_val = import_usd
          ) %>%
          mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
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
          mutate(prod_id_len = 6) %>%
          select(year, pairs, prod_id_len, export_val, import_val)
        
        yodp_6_t1 <- read_feather(clean_feather_list[[t]]) %>%
          rename(
            export_val = export_usd,
            import_val = import_usd
          ) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
          left_join(product_names) %>%
          rename(
            origin_id = id.x,
            dest_id = id.y
          ) %>%
          unite(pairs, origin_id, dest_id, prod_id) %>%
          mutate(prod_id_len = 8) %>%
          select(year, pairs, prod_id_len, export_val, import_val)
        
        yodp_t1 <- bind_rows(yodp_4_t1, yodp_6_t1)
        rm(yodp_4_t1, yodp_6_t1)
      } else {
        yodp_t1 <- read_feather(clean_feather_list[[t]]) %>%
          rename(
            export_val = export_usd,
            import_val = import_usd
          ) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
          left_join(product_names) %>%
          rename(
            origin_id = id.x,
            dest_id = id.y
          ) %>%
          unite(pairs, origin_id, dest_id, prod_id) %>%
          mutate(prod_id_len = 6) %>%
          select(year, pairs, prod_id_len, export_val, import_val)
      }
      
      if (t %in% match(years_missing_t_minus_1, years)) {
        yodp_t2 <- yodp_t1 %>% 
          select(pairs) %>% 
          mutate(
            export_val_t2 = NA,
            import_val_t2 = NA
          )
      } else {
        if (classification == "hs") {
          yodp_4_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            rename(
              export_val = export_usd,
              import_val = import_usd
            ) %>%
            mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
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
          
          yodp_6_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            rename(
              export_val_t2 = export_usd,
              import_val_t2 = import_usd
            ) %>%
            left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
            left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
            left_join(product_names) %>%
            rename(
              origin_id = id.x,
              dest_id = id.y
            ) %>%
            unite(pairs, origin_id, dest_id, prod_id) %>%
            select(pairs, export_val_t2, import_val_t2)
          
          yodp_t2 <- bind_rows(yodp_4_t2, yodp_6_t2)
          rm(yodp_4_t2, yodp_6_t2)
        } else {
          yodp_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            rename(
              export_val_t2 = export_usd,
              import_val_t2 = import_usd
            ) %>%
            left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
            left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
            left_join(product_names) %>%
            rename(
              origin_id = id.x,
              dest_id = id.y
            ) %>%
            unite(pairs, origin_id, dest_id, prod_id) %>%
            select(pairs, export_val_t2, import_val_t2)
        }
      }
      
      if (t %in% match(years_missing_t_minus_5, years) | t %in% match(years_missing_t_minus_1, years)) {
        yodp_t3 <- yodp_t1 %>% 
          select(pairs) %>% 
          mutate(
            export_val_t3 = NA,
            import_val_t3 = NA
          )
      } else {
        if (classification == "hs") {
          yodp_4_t3 <- read_feather(clean_feather_list[[t-5]]) %>%
            rename(
              export_val = export_usd,
              import_val = import_usd
            ) %>%
            mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
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
          
          yodp_6_t3 <- read_feather(clean_feather_list[[t-5]]) %>%
            rename(
              export_val_t3 = export_usd,
              import_val_t3 = import_usd
            ) %>%
            left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
            left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
            left_join(product_names) %>%
            rename(
              origin_id = id.x,
              dest_id = id.y
            ) %>%
            unite(pairs, origin_id, dest_id, prod_id) %>%
            select(pairs, export_val_t3, import_val_t3)
          
          yodp_t3 <- bind_rows(yodp_4_t3, yodp_6_t3)
          rm(yodp_4_t3, yodp_6_t3)
        } else {
          yodp_t3 <- read_feather(clean_feather_list[[t-5]]) %>%
            rename(
              export_val_t3 = export_usd,
              import_val_t3 = import_usd
            ) %>%
            left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
            left_join(country_names, by = c("partner_iso" = "id_3char")) %>%
            left_join(product_names) %>%
            rename(
              origin_id = id.x,
              dest_id = id.y
            ) %>%
            unite(pairs, origin_id, dest_id, prod_id) %>%
            select(pairs, export_val_t3, import_val_t3)
        }
      }
      
      yodp_t1 <- yodp_t1 %>% 
        mutate(
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        )
      
      if (class(yodp_t2$export_val_t2) != "logical") {
        yodp_t2 <- yodp_t2 %>% 
          mutate(
            export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
            import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
          )
      }
      
      if (class(yodp_t3$export_val_t3) != "logical") {
        yodp_t3 <- yodp_t3 %>% 
          mutate(
            export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
            import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
          )
      }
      
      yodp <- yodp_t1 %>%
        left_join(yodp_t2) %>%
        left_join(yodp_t3) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        separate(pairs, c("origin_id", "dest_id", "prod_id"))
      
      if (class(yodp$export_val_t2) != "numeric") {
        yodp <- yodp %>% 
          mutate(
            export_val_t2 = as.double(export_val_t2),
            export_val_t3 = as.double(export_val_t3),
            import_val_t2 = as.double(import_val_t2),
            import_val_t3 = as.double(import_val_t3)
          )
      }
      
      rm(yodp_t1, yodp_t2, yodp_t3)
      
      fwrite(
        yodp %>% 
          select(-c(matches("export_val_t"), matches("import_val_t"))), 
        yodp_csv_list[[t]]
      )
      
      # yod ------------------------------------------------------------------
      
      yod <- yodp %>%
        filter(prod_id_len == 6) %>% 
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
        ungroup()
      
      yod <- yod %>%
        mutate(
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        ) %>% 
        
        mutate(
          export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
          import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
        ) %>% 
        
        mutate(
          export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
          import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
        ) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yod, yod_csv_list[[t]])
      rm(yod)
      
      # yop ------------------------------------------------------------------
      
      if (classification == "hs") {
        rca_exp_4 <- read_feather(rca_exp_4_feather_list[[t]]) %>%
          left_join(product_names) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          select(-c(year, commodity_code, reporter_iso)) %>%
          rename(
            origin_id = id,
            export_rca = rca_export_smooth
          )
        
        rca_exp_6 <- read_feather(rca_exp_6_feather_list[[t]]) %>%
          left_join(product_names) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          select(-c(year, commodity_code, reporter_iso)) %>%
          rename(
            origin_id = id,
            export_rca = rca_export_smooth
          )
        
        rca_exp <- bind_rows(rca_exp_4, rca_exp_6)
        rm(rca_exp_4, rca_exp_6)
      } else {
        rca_exp <- read_feather(rca_exp_4_feather_list[[t]]) %>%
          left_join(product_names) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          select(-c(year, commodity_code, reporter_iso)) %>%
          rename(
            origin_id = id,
            export_rca = rca_export_smooth
          )
      }
      
      if (classification == "hs") {
        rca_imp_4 <- read_feather(rca_imp_4_feather_list[[t]]) %>%
          left_join(product_names) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          select(-c(year, commodity_code, reporter_iso)) %>%
          rename(
            origin_id = id,
            import_rca = rca_import_smooth
          )
        
        rca_imp_6 <- read_feather(rca_imp_6_feather_list[[t]]) %>%
          left_join(product_names) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          select(-c(year, commodity_code, reporter_iso)) %>%
          rename(
            origin_id = id,
            import_rca = rca_import_smooth
          )
        
        rca_imp <- bind_rows(rca_imp_4, rca_imp_6)
        rm(rca_imp_4, rca_imp_6)
      } else {
        rca_imp <- read_feather(rca_imp_4_feather_list[[t]]) %>%
          left_join(product_names) %>%
          left_join(country_names, by = c("reporter_iso" = "id_3char")) %>%
          select(-c(year, commodity_code, reporter_iso)) %>%
          rename(
            origin_id = id,
            import_rca = rca_import_smooth
          )
      }
      
      yop <- yodp %>%
        select(year, origin_id, prod_id, prod_id_len, matches("export_val"), matches("import_val")) %>%
        group_by(year, origin_id, prod_id, prod_id_len) %>%
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
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        ) %>% 
        
        mutate(
          export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
          import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
        ) %>% 
        
        mutate(
          export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
          import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
        ) %>%
        
        left_join(rca_exp) %>%
        left_join(rca_imp) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        select(year, origin_id, prod_id, prod_id_len, everything()) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yop, yop_csv_list[[t]])
      rm(yop, rca_exp, rca_imp)
      
      # ydp ------------------------------------------------------------------
      
      ydp <- yodp %>%
        select(year, dest_id, prod_id, prod_id_len, matches("export_val"), matches("import_val")) %>%
        group_by(year, dest_id, prod_id, prod_id_len) %>%
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
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        ) %>% 
        
        mutate(
          export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
          import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
        ) %>% 
        
        mutate(
          export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
          import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
        ) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        select(year, dest_id, prod_id, prod_id_len, everything()) %>%
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(ydp, ydp_csv_list[[t]])
      rm(ydp)
      
      # yo -------------------------------------------------------------------
      
      if (classification == "sitc") {
        max_exp_4 <- yodp %>%
          filter(prod_id_len == 6) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(export_val = sum(export_val, na.rm = T)) %>%
          group_by(origin_id) %>%
          slice(which.max(export_val)) %>%
          rename(
            top_export_sitc_id = prod_id,
            top_export_sitc = export_val
          )
        
        max_imp_4 <- yodp %>%
          filter(prod_id_len == 6) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(import_val = sum(import_val, na.rm = T)) %>%
          group_by(origin_id) %>%
          slice(which.max(import_val)) %>%
          rename(
            top_import_sitc_id = prod_id,
            top_import_sitc = import_val
          )
      }
      
      if (classification == "hs") {
        max_exp_4 <- yodp %>%
          filter(prod_id_len == 6) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(export_val = sum(export_val, na.rm = T)) %>%
          group_by(origin_id) %>%
          slice(which.max(export_val)) %>%
          rename(
            top_export_hs4_id = prod_id,
            top_export_hs4 = export_val
          )
        
        max_imp_4 <- yodp %>%
          filter(prod_id_len == 6) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(import_val = sum(import_val, na.rm = T)) %>%
          group_by(origin_id) %>%
          slice(which.max(import_val)) %>%
          rename(
            top_import_hs4_id = prod_id,
            top_import_hs4 = import_val
          )
        
        max_exp_6 <- yodp %>%
          filter(prod_id_len == 8) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(export_val = sum(export_val, na.rm = T)) %>%
          group_by(origin_id) %>%
          slice(which.max(export_val)) %>%
          rename(
            top_export_hs6_id = prod_id,
            top_export_hs6 = export_val
          )
        
        max_imp_6 <- yodp %>%
          filter(prod_id_len == 8) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(import_val = sum(import_val, na.rm = T)) %>%
          group_by(origin_id) %>%
          slice(which.max(import_val)) %>%
          rename(
            top_import_hs6_id = prod_id,
            top_import_hs6 = import_val
          )
      }
      
      yo <- yodp %>%
        filter(prod_id_len == 6) %>% 
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
        ungroup() 
      
      if (classification == "hs") {
        yo <- yo %>%
          left_join(max_exp_4, by = "origin_id") %>%
          left_join(max_imp_4, by = "origin_id") %>% 
          left_join(max_exp_6, by = "origin_id") %>%
          left_join(max_imp_6, by = "origin_id")
      } else {
        yo <- yo %>%
          left_join(max_exp_4, by = "origin_id") %>%
          left_join(max_imp_4, by = "origin_id")
      }
      
      yo <- yo %>%
        mutate(
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        ) %>% 
        
        mutate(
          export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
          import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
        ) %>% 
        
        mutate(
          export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
          import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
        ) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yo, yo_csv_list[[t]])
      rm(yo, max_exp_4, max_imp_4)
      try(rm(max_exp_6, max_imp_6))
      
      # yd -------------------------------------------------------------------
      
      yd <- yodp %>%
        filter(prod_id_len == 6) %>% 
        select(year, origin_id, matches("export_val"), matches("import_val")) %>%
        rename(dest_id = origin_id) %>% 
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
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        ) %>% 
        
        mutate(
          export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
          import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
        ) %>% 
        
        mutate(
          export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
          import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
        ) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        select(-c(matches("export_val_t"), matches("import_val_t")))
      
      fwrite(yd, yd_csv_list[[t]])
      rm(yd)
      
      # yp -------------------------------------------------------------------
      
      pci_t1 <- pci %>%
        filter(year == years[[t]]) %>%
        left_join(product_names) %>%
        select(-c(year, commodity_code))
      
      if (t %in% match(years_missing_t_minus_1, years)) {
        pci_t2 <- pci_t1 %>% 
          mutate(
            pci_rank = NA,
            pci = NA
          )
      } else {
        pci_t2 <- pci %>%
          filter(year == years[[t-1]]) %>%
          left_join(product_names) %>%
          select(-c(year, commodity_code))
      }
      
      if (classification == "hs") {
        max_exp_4 <- yodp %>%
          filter(prod_id_len == 6) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(export_val = sum(export_val, na.rm = T)) %>%
          group_by(prod_id) %>%
          slice(which.max(export_val)) %>%
          rename(top_exporter_id = origin_id) %>%
          select(-export_val)
        
        max_imp_4 <- yodp %>%
          filter(prod_id_len == 6) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(import_val = sum(import_val, na.rm = T)) %>%
          group_by(prod_id) %>%
          slice(which.max(import_val)) %>%
          rename(top_importer_id = origin_id) %>%
          select(-import_val)
        
        max_exp_6 <- yodp %>%
          filter(prod_id_len == 8) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(export_val = sum(export_val, na.rm = T)) %>%
          group_by(prod_id) %>%
          slice(which.max(export_val)) %>%
          rename(top_exporter_id = origin_id) %>%
          select(-export_val)
        
        max_imp_6 <- yodp %>%
          filter(prod_id_len == 8) %>% 
          group_by(origin_id, prod_id) %>%
          summarise(import_val = sum(import_val, na.rm = T)) %>%
          group_by(prod_id) %>%
          slice(which.max(import_val)) %>%
          rename(top_importer_id = origin_id) %>%
          select(-import_val)
        
        max_exp <- bind_rows(max_exp_4, max_exp_6)
        max_imp <- bind_rows(max_imp_4, max_imp_6)
        rm(max_exp_4, max_exp_6, max_imp_4, max_imp_6)
      } else {
        max_exp <- yodp %>%
          group_by(origin_id, prod_id) %>%
          summarise(export_val = sum(export_val, na.rm = T)) %>%
          group_by(prod_id) %>%
          slice(which.max(export_val)) %>%
          rename(top_exporter_id = origin_id) %>%
          select(-export_val)
        
        max_imp <- yodp %>%
          group_by(origin_id, prod_id) %>%
          summarise(import_val = sum(import_val, na.rm = T)) %>%
          group_by(prod_id) %>%
          slice(which.max(import_val)) %>%
          rename(top_importer_id = origin_id) %>%
          select(-import_val)
      }
      
      yp <- yodp %>%
        select(year, prod_id, prod_id_len, matches("export_val"), matches("import_val")) %>%
        group_by(year, prod_id, prod_id_len) %>%
        summarise(
          export_val = sum(export_val, na.rm = T),
          import_val = sum(import_val, na.rm = T),
          export_val_t2 = sum(export_val_t2, na.rm = T),
          import_val_t2 = sum(import_val_t2, na.rm = T),
          export_val_t3 = sum(export_val_t3, na.rm = T),
          import_val_t3 = sum(import_val_t3, na.rm = T)
        ) %>%
        ungroup() 
      
      yp <- yp %>%
        left_join(pci_t1, by = "prod_id") %>%
        left_join(pci_t2, by = "prod_id") %>%
        mutate(pci_rank_delta = pci_rank.x - pci_rank.y) %>%
        select(-c(pci_rank.y, pci.y)) %>% 
        rename(pci = pci.x,
               pci_rank = pci_rank.x)
        
      yp <- yp %>%
        left_join(max_exp, by = "prod_id") %>%
        left_join(max_imp, by = "prod_id")
      
      yp <- yp %>% 
        mutate(
          export_val = ifelse(export_val == 0, NA, export_val),
          import_val = ifelse(import_val == 0, NA, import_val)
        ) %>% 
        
        mutate(
          export_val_t2 = ifelse(export_val_t2 == 0, NA, export_val_t2),
          import_val_t2 = ifelse(import_val_t2 == 0, NA, import_val_t2)
        ) %>% 
        
        mutate(
          export_val_t3 = ifelse(export_val_t3 == 0, NA, export_val_t3),
          import_val_t3 = ifelse(import_val_t3 == 0, NA, import_val_t3)
        ) %>%
        
        mutate(
          export_val_growth_val = export_val - export_val_t2,
          export_val_growth_val_5 = export_val - export_val_t3
        ) %>% 
        
        mutate(
          export_val_growth_pct = export_val_growth_val / export_val_t2,
          export_val_growth_pct_5 = export_val_growth_val / export_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_val = import_val - import_val_t2,
          import_val_growth_val_5 = import_val - import_val_t3
        ) %>% 
        
        mutate(
          import_val_growth_pct = import_val_growth_val / import_val_t2,
          import_val_growth_pct_5 = import_val_growth_val / import_val_t3
        ) %>%
        
        select(-c(matches("export_val_t"), matches("import_val_t"))) %>%
        select(year, prod_id, prod_id_len, everything())
      
      fwrite(yp, yp_csv_list[[t]])
      rm(yodp, yp, pci_t1, pci_t2, max_exp, max_imp)
    }
  }
  
  stopCluster(cl)
  rm(cl)
  
  # compress output ---------------------------------------------------------
  
  tables_csv_list <- list.files(tables_dir, pattern = "csv", full.names = T, recursive = T)
  tables_zip_list <- gsub("csv", "zip", tables_csv_list)
  
  mclapply(1:length(tables_csv_list), compress, mc.cores = n_cores)
  
  lapply(clean_feather_list, file.remove2)
  
  try(unlink(rca_exp_4_dir, recursive = T))
  try(unlink(rca_imp_4_dir, recursive = T))
  
  if (classification == "hs") {
    try(unlink(rca_exp_6_dir, recursive = T))
    try(unlink(rca_imp_6_dir, recursive = T))
  }
}

tables()
