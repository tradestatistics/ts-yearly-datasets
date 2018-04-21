# Open oec-yearly-data.Rproj before running this function

measures <- function() {
  
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
  n_cores <- ceiling(detectCores() / 2)
  
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
    years_missing_t_minus_1 <- 1990; years_missing_t_minus_2 <- 1991; years_full <- 1992:2016
  }
  if (classification == "hs" & rev == 1992) {
    years_missing_t_minus_1 <- 1992; years_missing_t_minus_2 <- 1993; years_full <- 1994:2016
  }
  if (classification == "hs" & rev == 1996) {
    years_missing_t_minus_1 <- 1996; years_missing_t_minus_2 <- 1997; years_full <- 1998:2016
  }
  if (classification == "hs" & rev == 2002) {
    years_missing_t_minus_1 <- 2002; years_missing_t_minus_2 <- 2003; years_full <- 2004:2016
  }
  if (classification == "hs" & rev == 2007) {
    years_missing_t_minus_1 <- 2007; years_missing_t_minus_2 <- 2008; years_full <- 2009:2016
  }
  
  # input dir ---------------------------------------------------------------
  
  clean_dir <- "1-2-clean-data"
  clean_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, rev)
  
  # output dirs -------------------------------------------------------------
  
  measures_dir <- "1-3-measures"
  measures_dir <- sprintf("%s/%s-rev%s-%s", measures_dir, classification, rev, J)
  try(dir.create(measures_dir))
  
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
      system(paste0("7z e -aos ", clean_zip_list[[t]], " -oc:", measures_dir))
    }
  }
  
  file.remove2 <- function(t) {
    try(file.remove(t))
  }
  
  compress <- function(t) {
    if (file.exists(measures_zip_list[[t]])) {
      message("The compressed files exist. Skipping.")
    } else {
      messageline()
      system(paste("7z a", measures_zip_list[[t]], measures_feather_list[[t]]))
      file.remove2(measures_feather_list[[t]])
    }
  }
  
  Rcpp::sourceCpp("0-scripts/matrices-1.cpp")
  Rcpp::sourceCpp("0-scripts/matrices-2.cpp")
  
  # list input files --------------------------------------------------------
  
  clean_zip_list <-
    list.files(path = clean_dir,
               pattern = "\\.zip",
               full.names = T) %>%
    grep(paste(years, collapse = "|"), ., value = TRUE)
  
  # list output files -------------------------------------------------------
  
  clean_feather_list <- clean_zip_list %>% 
    gsub("zip", "feather", .) %>% 
    gsub(clean_dir, measures_dir, .)
  
  # uncompress input --------------------------------------------------------
  
  mclapply(1:length(clean_feather_list), extract, mc.cores = n_cores)
  
  # smooth RCA exports ------------------------------------------------------
  
  rca_exports_dir <- sprintf("%s/smooth-rca-exports", measures_dir)
  try(dir.create(rca_exports_dir))
  rca_exports_list <- sprintf("%s/smooth-rca-exports-%s.feather", rca_exports_dir, years)
  rca_exports_zip_list <- gsub("feather", "zip", rca_exports_list)
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = 1:length(years)) %dopar% {
    
    if (file.exists(rca_exports_list[[t]])) {
      messageline()
      message(paste0("Skipping year ", years[[t]], ". The file already exist."
      ))
    } else {
      messageline()
      message(paste0(
        "Creating smooth RCA file for the year ", years[[t]], ". Be patient..."
      ))
      
      if (classification == "hs" & J == 4) {
        exports_t1 <- read_feather(clean_feather_list[[t]]) %>%
          select(-c(partner_iso, import_usd)) %>%
          mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
          group_by(year, reporter_iso, commodity_code) %>% 
          summarise(export_usd = sum(export_usd, na.rm = T)) %>% 
          ungroup() %>% 
          unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
          rename(exports_usd_t1 = export_usd)
      } else {
        exports_t1 <- read_feather(clean_feather_list[[t]]) %>%
          select(-c(partner_iso, import_usd)) %>%
          group_by(year, reporter_iso, commodity_code) %>% 
          summarise(export_usd = sum(export_usd, na.rm = T)) %>% 
          ungroup() %>% 
          unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
          rename(exports_usd_t1 = export_usd)
      }
      
      if (years[[t]] <= years_missing_t_minus_1) {
        exports_t2 <- exports_t1 %>% 
          select(pairs) %>% 
          mutate(exports_usd_t2 = NA)
      } else {
        if (classification == "hs" & J == 4) {
          exports_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            select(-c(year, partner_iso, import_usd)) %>%
            mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
            group_by(reporter_iso, commodity_code) %>% 
            summarise(export_usd = sum(export_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(exports_usd_t2 = export_usd) %>% 
            select(pairs, exports_usd_t2)
        } else {
          exports_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            select(-c(year, partner_iso, import_usd)) %>%
            group_by(reporter_iso, commodity_code) %>% 
            summarise(export_usd = sum(export_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(exports_usd_t2 = export_usd) %>% 
            select(pairs, exports_usd_t2)
        }
      }
      
      if (years[[t]] <= years_missing_t_minus_2) {
        exports_t3 <- exports_t1 %>% 
          select(pairs) %>% 
          mutate(exports_usd_t3 = NA)
      } else {
        if (classification == "hs" & J == 4) {
          exports_t3 <- read_feather(clean_feather_list[[t-2]]) %>%
            select(-c(year, partner_iso, import_usd)) %>%
            mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
            group_by(reporter_iso, commodity_code) %>% 
            summarise(export_usd = sum(export_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(exports_usd_t3 = export_usd) %>% 
            select(pairs, exports_usd_t3)
        } else {
          exports_t3 <- read_feather(clean_feather_list[[t-2]]) %>%
            select(-c(year, partner_iso, import_usd)) %>%
            group_by(reporter_iso, commodity_code) %>% 
            summarise(export_usd = sum(export_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(exports_usd_t3 = export_usd) %>% 
            select(pairs, exports_usd_t3)
        } 
      }
      
      exports_t1 <- exports_t1 %>% 
        left_join(exports_t2, by = "pairs") %>%
        left_join(exports_t3, by = "pairs") %>% 
        rowwise() %>% # To apply a weighted mean by rows with 1 weight = 1 column
        mutate(
          export_usd_smooth = weighted.mean(
            x = c(exports_usd_t1, exports_usd_t2, exports_usd_t3),
            w = c(2, 1, 1),
            na.rm = TRUE)
        ) %>% 
        ungroup() %>% 
        select(-c(pairs, exports_usd_t1, exports_usd_t2, exports_usd_t3)) %>% 
        group_by(commodity_code) %>% # Sum by product
        mutate(sum_p_xcp = sum(export_usd_smooth, na.rm = TRUE)) %>% 
        ungroup() %>% 
        group_by(reporter_iso) %>% # Sum by country
        mutate(sum_c_xcp = sum(export_usd_smooth, na.rm = TRUE)) %>% 
        ungroup() %>% 
        group_by(reporter_iso, commodity_code) %>% # Sum country exports by product
        mutate(xcp = sum(export_usd_smooth, na.rm = TRUE)) %>% 
        ungroup() %>% 
        distinct(reporter_iso, commodity_code, .keep_all = TRUE) %>%
        mutate(sum_c_p_xcp = sum(xcp, na.rm = TRUE)) %>%  # World's total exported value
        mutate(rca_export_smooth = (xcp / sum_c_xcp) / (sum_p_xcp / sum_c_p_xcp)) %>%  # Compute RCA
        select(year, reporter_iso, commodity_code, rca_export_smooth)
      
      write_feather(exports_t1, rca_exports_list[[t]])
      rm(exports_t1, exports_t2, exports_t3)
    }
  }
  
  stopCluster(cl)
  rm(cl)
  
  # smooth RCA imports ------------------------------------------------------
  
  rca_imports_dir <- sprintf("%s/smooth-rca-imports", measures_dir)
  try(dir.create(rca_imports_dir))
  rca_imports_list <- sprintf("%s/smooth-rca-imports-%s.feather", rca_imports_dir, years)
  rca_imports_zip_list <- gsub("feather", "zip", rca_imports_list)
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = 1:length(years)) %dopar% {
    
    if (file.exists(rca_imports_list[[t]])) {
      messageline()
      message(paste0("Skipping year ", years[[t]], ". The file already exist."
      ))
    } else {
      messageline()
      message(paste0(
        "Creating smooth RCA file for the year ", years[[t]], ". Be patient..."
      ))
      
      if (classification == "hs" & J == 4) {
        imports_t1 <- read_feather(clean_feather_list[[t]]) %>%
          select(-c(partner_iso, export_usd)) %>%
          mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
          group_by(year, reporter_iso, commodity_code) %>% 
          summarise(import_usd = sum(import_usd, na.rm = T)) %>% 
          ungroup() %>% 
          unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
          rename(imports_usd_t1 = import_usd)
      } else {
        imports_t1 <- read_feather(clean_feather_list[[t]]) %>%
          select(-c(partner_iso, export_usd)) %>%
          group_by(year, reporter_iso, commodity_code) %>% 
          summarise(import_usd = sum(import_usd, na.rm = T)) %>% 
          ungroup() %>% 
          unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
          rename(imports_usd_t1 = import_usd)
      }
      
      if (years[[t]] <= years_missing_t_minus_1) {
        imports_t2 <- imports_t1 %>% 
          select(pairs) %>% 
          mutate(imports_usd_t2 = NA)
      } else {
        if (classification == "hs" & J == 4) {
          imports_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            select(-c(year, partner_iso, export_usd)) %>%
            mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
            group_by(reporter_iso, commodity_code) %>% 
            summarise(import_usd = sum(import_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(imports_usd_t2 = import_usd) %>% 
            select(pairs, imports_usd_t2)
        } else {
          imports_t2 <- read_feather(clean_feather_list[[t-1]]) %>%
            select(-c(year, partner_iso, export_usd)) %>%
            group_by(reporter_iso, commodity_code) %>% 
            summarise(import_usd = sum(import_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(imports_usd_t2 = import_usd) %>% 
            select(pairs, imports_usd_t2)
        }
      }
      
      if (years[[t]] <= years_missing_t_minus_2) {
        imports_t3 <- imports_t1 %>% 
          select(pairs) %>% 
          mutate(imports_usd_t3 = NA)
      } else {
        if (classification == "hs" & J == 4) {
          imports_t3 <- read_feather(clean_feather_list[[t-2]]) %>%
            select(-c(year, partner_iso, export_usd)) %>%
            mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
            group_by(reporter_iso, commodity_code) %>% 
            summarise(import_usd = sum(import_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(imports_usd_t3 = import_usd) %>% 
            select(pairs, imports_usd_t3)
        } else {
          imports_t3 <- read_feather(clean_feather_list[[t-2]]) %>%
            select(-c(year, partner_iso, export_usd)) %>%
            group_by(reporter_iso, commodity_code) %>% 
            summarise(import_usd = sum(import_usd, na.rm = T)) %>% 
            ungroup() %>% 
            unite(pairs, reporter_iso, commodity_code, remove = FALSE) %>% 
            rename(imports_usd_t3 = import_usd) %>% 
            select(pairs, imports_usd_t3)
        } 
      }
      
      imports_t1 <- imports_t1 %>% 
        left_join(imports_t2, by = "pairs") %>%
        left_join(imports_t3, by = "pairs") %>% 
        rowwise() %>% # To apply a weighted mean by rows with 1 weight = 1 column
        mutate(
          import_usd_smooth = weighted.mean(
            x = c(imports_usd_t1, imports_usd_t2, imports_usd_t3),
            w = c(2, 1, 1),
            na.rm = TRUE)
        ) %>% 
        ungroup() %>% 
        select(-c(pairs, imports_usd_t1, imports_usd_t2, imports_usd_t3)) %>% 
        group_by(commodity_code) %>% # Sum by product
        mutate(sum_p_mcp = sum(import_usd_smooth, na.rm = TRUE)) %>% 
        ungroup() %>% 
        group_by(reporter_iso) %>% # Sum by country
        mutate(sum_c_mcp = sum(import_usd_smooth, na.rm = TRUE)) %>% 
        ungroup() %>% 
        group_by(reporter_iso, commodity_code) %>% # Sum country imports by product
        mutate(mcp = sum(import_usd_smooth, na.rm = TRUE)) %>% 
        ungroup() %>% 
        distinct(reporter_iso, commodity_code, .keep_all = TRUE) %>%
        mutate(sum_c_p_mcp = sum(mcp, na.rm = TRUE)) %>%  # World's total imported value
        mutate(rca_import_smooth = (mcp / sum_c_mcp) / (sum_p_mcp / sum_c_p_mcp)) %>%  # Compute RCA
        select(year, reporter_iso, commodity_code, rca_import_smooth)
      
      write_feather(imports_t1, rca_imports_list[[t]])
      rm(imports_t1, imports_t2, imports_t3)
    }
  }
  
  stopCluster(cl)
  rm(cl)
  
  # RCA based measures ----------------------------------------------------
  
  ranking_1 <- as_tibble(fread("../oec-atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv")) %>%
    mutate(iso_code = tolower(iso_code)) %>%
    rename(reporter_iso = iso_code)
  
  eci_dir <- sprintf("%s/eci", measures_dir)
  try(dir.create(eci_dir))
  eci_rankings_list <- sprintf("%s/eci-%s.feather", eci_dir, years)
  
  pci_dir <- sprintf("%s/pci", measures_dir)
  try(dir.create(pci_dir))
  pci_rankings_list <- sprintf("%s/pci-%s.feather", pci_dir, years)
  
  proximity_countries_dir <- sprintf("%s/proximity-countries", measures_dir)
  try(dir.create(proximity_countries_dir))
  proximity_countries_list <- sprintf("%s/proximity-countries-%s.feather", proximity_countries_dir, years)
  
  proximity_products_dir <- sprintf("%s/proximity-products", measures_dir)
  try(dir.create(proximity_products_dir))
  proximity_products_list <- sprintf("%s/proximity-products-%s.feather", proximity_products_dir, years)
  
  density_countries_dir <- sprintf("%s/density-countries", measures_dir)
  try(dir.create(density_countries_dir))
  density_countries_list <- sprintf("%s/density-countries-%s.feather", density_countries_dir, years)
  
  density_products_dir <- sprintf("%s/density-products", measures_dir)
  try(dir.create(density_products_dir))
  density_products_list <- sprintf("%s/density-products-%s.feather", density_products_dir, years)
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  foreach (t = 1:length(years)) %dopar% {
    
    # RCA matrix --------------------------------------------------------------
    
    rca_matrix <- read_feather(rca_exports_list[[t]]) %>%
      select(c(reporter_iso, commodity_code, rca_export_smooth)) %>%
      inner_join(ranking_1 %>% select(reporter_iso)) %>% 
      mutate(rca_export_smooth = ifelse(rca_export_smooth > 1, 1, 0)) %>% 
      spread(commodity_code, rca_export_smooth)
    
    diversity <- rca_matrix %>% select(reporter_iso)
    ubiquity <- tibble(product = colnames(rca_matrix)) %>% filter(row_number() > 1)
    
    rca_matrix <- rca_matrix %>% 
      select(-reporter_iso) %>% 
      as.matrix()
    
    # convert to sparse class
    rca_matrix[is.na(rca_matrix)] <- 0
    rca_matrix <- Matrix(rca_matrix, sparse = T)
    
    diversity <- diversity %>%
      mutate(val = rowSums(rca_matrix, na.rm = TRUE)) %>%
      filter(val > 0)
    
    ubiquity <- ubiquity %>%
      mutate(val = colSums(rca_matrix, na.rm = TRUE)) %>%
      filter(val > 0)
    
    rownames(rca_matrix) <- diversity$reporter_iso
    
    D <- as.matrix(diversity$val, ncol = 1)
    U <- as.matrix(ubiquity$val, ncol = 1)
    
    # remove null rows and cols
    Mcp <- rca_matrix[which(rownames(rca_matrix) %in% unlist(diversity$reporter_iso)) , which(colnames(rca_matrix) %in% unlist(ubiquity$product))]
    rm(rca_matrix)
    
    # diversity and ubiquity following the Atlas notation
    kc0 <- as.numeric(D)
    kp0 <- as.numeric(U)
    
    # reflections method ------------------------------------------------------
    
    kcinv <- 1 / kc0
    kpinv <- 1 / kp0
    
    # create empty matrices
    kc <- Matrix(0, nrow = length(kc0), ncol = 20, sparse = T)
    kp <- Matrix(0, nrow = length(kp0), ncol = 20, sparse = T)
    
    # fill the first column with kc0 and kp0 to start iterating
    kc[ ,1] <- kc0
    kp[ ,1] <- kp0
    
    # compute cols 2 to 20 by iterating from col 1
    for (c in 2:ncol(kc)) {
      kc[ ,c] <- kcinv * (Mcp %*% kp[ ,(c - 1)])
      kp[ ,c] <- kpinv * (t(Mcp) %*% kc[ ,(c - 1)])
    }
    
    # ECI (reflections method) ------------------------------------------------
    
    eci_reflections <- as_tibble(
      (kc[ ,19] - mean(kc[ ,19])) / sd(kc[ ,19])
    ) %>%
      mutate(country_iso = diversity$reporter_iso) %>%
      mutate(year = years[[t]]) %>%
      select(year, country_iso, value) %>%
      arrange(desc(value)) %>% 
      rename(eci = value)
    
    write_feather(eci_reflections, eci_rankings_list[[t]])
    
    # PCI (reflections method) ------------------------------------------------
    
    pci_reflections <- as_tibble(
      (kp[, 20] - mean(kp[ ,20])) / sd(kp[ ,20])
    ) %>%
      mutate(commodity_code = ubiquity$product) %>%
      mutate(year = years[[t]]) %>%
      select(year, commodity_code, value) %>%
      arrange(desc(value)) %>% 
      rename(pci = value)

    write_feather(pci_reflections, pci_rankings_list[[t]])
    
    rm(
      kc0,
      kp0,
      kcinv,
      kpinv,
      kc,
      kp,
      eci_reflections,
      pci_reflections,
      c
    )
    
    # proximity (countries) ---------------------------------------------------
    
    Phi_cc <- (Mcp %*% t(Mcp)) / proximity_countries_denominator(Mcp, D, cores = n_cores)
    
    Phi_cc_l <- Phi_cc
    Phi_cc_l[upper.tri(Phi_cc_l, diag = T)] <- NA
    
    Phi_cc_long <- as_tibble(as.matrix(Phi_cc_l)) %>% 
      mutate(id = rownames(Phi_cc)) %>% 
      gather(id2, value, -id) %>% 
      filter(!is.na(value)) %>% 
      setNames(c("country_iso", "country_iso_2", "value"))
    
    write_feather(Phi_cc_long, proximity_countries_list[[t]])
    rm(Phi_cc_l, Phi_cc_long)
    
    # proximity (products) ---------------------------------------------------
    
    Phi_pp <- (t(Mcp) %*% Mcp) / proximity_products_denominator(Mcp, U, cores = n_cores)
    
    Phi_pp_l <- Phi_pp
    Phi_pp_l[upper.tri(Phi_pp_l, diag = T)] <- NA
    
    Phi_pp_long <- as_tibble(as.matrix(Phi_pp_l)) %>% 
      mutate(id = rownames(Phi_pp)) %>% 
      gather(id2, value, -id) %>% 
      filter(!is.na(value)) %>% 
      setNames(c(paste0("product_", classification, rev, "_id"), 
                 paste0("product_", classification, rev, "_id_2"),
                 "value"))
    
    write_feather(Phi_pp_long, proximity_products_list[[t]])
    rm(Phi_pp_l, Phi_pp_long)
    
    # density (countries) -----------------------------------------------------
    
    Omega_countries_cp <- (Phi_cc %*% Mcp) / colSums(Phi_cc)
    
    Omega_countries_cp_long <- as_tibble(as.matrix(Omega_countries_cp)) %>% 
      mutate(country_iso = rownames(Omega_countries_cp)) %>% 
      gather(product, value, -country_iso) %>% 
      setNames(c("country_iso", paste0("product_", classification, rev, "_id"), "value"))
    
    write_feather(Omega_countries_cp_long, density_countries_list[[t]])
    rm(Omega_countries_cp, Omega_countries_cp_long)
    
    # density (products) ------------------------------------------------------
    
    Omega_products_cp <- t((Phi_pp %*% t(Mcp)) / colSums(Phi_pp))
    
    Omega_products_cp_long <- as_tibble(as.matrix(Omega_products_cp)) %>% 
      mutate(country_iso = rownames(Omega_products_cp)) %>% 
      gather(product, value, -country_iso) %>% 
      setNames(c("country_iso", paste0("product_", classification, rev, "_id"), "value"))
    
    write_feather(Omega_products_cp_long, density_products_list[[t]])
    rm(Omega_products_cp, Omega_products_cp_long)
    
    rm(Mcp, Phi_pp, Phi_cc, ubiquity, diversity, D, U)
  }
  
  stopCluster(cl)
  rm(cl)
  
  # join ECI rankings -------------------------------------------------------
  
  joined_eci_rankings_full <- mclapply(eci_rankings_list, read_feather, mc.cores = n_cores)
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  joined_eci_rankings_full <- foreach (t = 1:length(years)) %dopar% {
    joined_eci_rankings_full[[t]] %>%
      mutate(eci_rank = row_number()) %>%
      select(year, everything())
  }
  
  stopCluster(cl)
  rm(cl)
  
  joined_eci_rankings_full <- bind_rows(joined_eci_rankings_full)
  fwrite(joined_eci_rankings_full, paste0(measures_dir, "/eci-joined-ranking.csv"))
  rm(joined_eci_rankings_full)
  
  # join PCI rankings -------------------------------------------------------
  
  joined_pci_rankings_full <- mclapply(pci_rankings_list, read_feather, mc.cores = n_cores)
  
  cl <- makeCluster(n_cores, type = "FORK")
  registerDoParallel(cores = n_cores)
  
  joined_pci_rankings_full <- foreach (t = 1:length(years)) %dopar% {
    joined_pci_rankings_full[[t]] %>%
      mutate(pci_rank = row_number()) %>%
      select(year, everything())
  }
  
  stopCluster(cl)
  rm(cl)
  
  joined_pci_rankings_full <- bind_rows(joined_pci_rankings_full)
  fwrite(joined_pci_rankings_full, paste0(measures_dir, "/pci-joined-ranking.csv"))
  rm(joined_pci_rankings_full)
  
  # remove uncompressed input files -----------------------------------------

  try(file.remove(clean_feather_list))
  
  # compress output ---------------------------------------------------------
  
  measures_feather_list <- list.files(measures_dir, pattern = "feather", full.names = T, recursive = T)
  measures_zip_list <- gsub("feather", "zip", measures_feather_list)
  
  mclapply(1:length(measures_feather_list), compress, mc.cores = n_cores)
}

measures()
