# Open ts-yearly-datasets.Rproj before running this function

complexity <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2021, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # helpers -----------------------------------------------------------------

  ask_number_of_cores <<- 1
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")

  # functions ---------------------------------------------------------------

  compute_complexity_measures <- function(x, yr, ye, yf, zr, ze, zf, q, w, t) {
    # RCA data ----
    
    rca_data <- readRDS(x[t]) %>%
      inner_join(select(ranking_1, reporter_iso), by = c("country_iso" = "reporter_iso")) %>%
      mutate(export_rca = ifelse(export_rca > 1, 1, 0)) %>%
      select(-year)
    
    # ECI/PCI 4 digits ----
    
    names(rca_data) <- c("country", "product", "value")
    
    rca_data <- rca_data %>% 
      spread(product, value) %>% 
      as.data.frame()
    
    rownames_rca_data <- rca_data[, 1]
    
    rca_data <- as.matrix(rca_data[, -1])
    rownames(rca_data) <- rownames_rca_data
    rca_data <- Matrix(rca_data, sparse = TRUE)
    
    if (!file.exists(yr[t])) {
      reflections <- complexity_measures(rca_data, method = "reflections")
      
      reflections$complexity_index_country <- reflections$complexity_index_country %>% 
        enframe(name = "country", value = "eci")
      
      reflections$complexity_index_product <- reflections$complexity_index_product %>% 
        enframe(name = "product", value = "pci")
    }
    
    if (!file.exists(ye[t])) {
      eigenvalues <- complexity_measures(rca_data, method = "eigenvalues")
      
      eigenvalues$complexity_index_country <- eigenvalues$complexity_index_country %>% 
        enframe(name = "country", value = "eci")
      
      eigenvalues$complexity_index_product <- eigenvalues$complexity_index_product %>% 
        enframe(name = "product", value = "pci")
    }
    
    if (!file.exists(yf[t])) {
      fitness <- complexity_measures(rca_data, method = "fitness")
      
      fitness$complexity_index_country <- fitness$complexity_index_country %>% 
        enframe(name = "country", value = "eci")
      
      fitness$complexity_index_product <- fitness$complexity_index_product %>% 
        enframe(name = "product", value = "pci")
    }
    
    # save ECI ----
    
    if (!file.exists(yr[t])) {
      saveRDS(reflections$complexity_index_country, file = yr[t], compress = "xz")
    }
    
    if (!file.exists(ye[t])) {
      saveRDS(eigenvalues$complexity_index_country, file = ye[t], compress = "xz")
    }
    
    if (!file.exists(yf[t])) {
      saveRDS(fitness$complexity_index_country, file = yf[t], compress = "xz")
    }
    
    # save PCI ----
    
    if (!file.exists(zr[t])) {
      saveRDS(reflections$complexity_index_product, file = zr[t], compress = "xz")
    }
    
    if (!file.exists(ze[t])) {
      saveRDS(eigenvalues$complexity_index_product, file = ze[t], compress = "xz")
    }
    
    if (!file.exists(zf[t])) {
      saveRDS(fitness$complexity_index_product, file = zf[t], compress = "xz")
    }
    
    # proximity ----
    
    if (!file.exists(q[t]) | !file.exists(w[t])) {
      pro <- proximity(rca_data)
      
      pro$proximity_country <- pro$proximity_country %>% 
        as.matrix() %>% 
        as.data.frame() %>% 
        tibble::rownames_to_column("country1") %>% 
        gather("country2", "proximity", -country1) %>% 
        as_tibble()
      
      pro$proximity_product <- pro$proximity_product %>% 
        as.matrix() %>% 
        as.data.frame() %>% 
        tibble::rownames_to_column("product1") %>% 
        gather("product2", "proximity", -product1) %>% 
        as_tibble()
    }
    
    if (!file.exists(q[t])) {
      saveRDS(pro$proximity_country, file = q[t], compress = "xz")
    }
    
    if (!file.exists(w[t])) {
      saveRDS(pro$proximity_product, file = w[t], compress = "xz")
    }
  }
  
  # RCA based measures ----

  ranking_1 <<- fread("../atlas-data/2-scraped-tables/ranking-1-economic-complexity-index.csv") %>%
    mutate(
      iso_code = tolower(iso_code),
      iso_code = ifelse(iso_code == "rou", "rom", iso_code)
    ) %>%
    rename(reporter_iso = iso_code) %>% 
    as_tibble()

  if (operating_system != "Windows") {
    mclapply(seq_along(years_full),
      compute_complexity_measures,
      x = rca_exports_rds,
      yr = eci_rankings_r_rds,
      ye = eci_rankings_e_rds,
      yf = eci_rankings_f_rds,
      zr = pci_rankings_r_rds,
      ze = pci_rankings_e_rds,
      zf = pci_rankings_f_rds,
      q = proximity_countries_rds,
      w = proximity_products_rds,
      mc.cores = n_cores
    )
  } else {
    lapply(seq_along(years_full),
      compute_complexity_measures,
      x = rca_exports_rds,
      yr = eci_rankings_r_rds,
      ye = eci_rankings_e_rds,
      yf = eci_rankings_f_rds,
      zr = pci_rankings_r_rds,
      ze = pci_rankings_e_rds,
      zf = pci_rankings_f_rds,
      q = proximity_countries_rds,
      w = proximity_products_rds
    )
  }
  
  # join ECI rankings ----
  
  tidy_eci <- function(d,t) {
    readRDS(d) %>% 
      arrange(-eci) %>%
      mutate(
        year = t,
        eci_rank = row_number()
      ) %>%
      select(year, everything())
  }
  
  joined_eci_ranking <- list(
    reflections = map2(eci_rankings_r_rds, years_full, tidy_eci),
    eigenvalues = map2(eci_rankings_e_rds, years_full, tidy_eci),
    fitness = map2(eci_rankings_f_rds, years_full, tidy_eci)
  )
  
  write_eci <- function(x,y) {
    d <- bind_rows(joined_eci_ranking[[x]])
    saveRDS(d, file = y, compress = "xz")
  }
  
  map2(seq_along(joined_eci_ranking), eci_files, write_eci)
  
  rm(joined_eci_ranking)
  
  # join PCI rankings ----
  
  tidy_pci <- function(d,t) {
    readRDS(d) %>% 
      arrange(-pci) %>%
      mutate(
        year = t,
        pci_rank = row_number()
      ) %>%
      select(year, everything())
  }
  
  joined_pci_ranking <- list(
    reflections = map2(pci_rankings_r_rds, years_full, tidy_pci),
    eigenvalues = map2(pci_rankings_e_rds, years_full, tidy_pci),
    fitness = map2(pci_rankings_f_rds, years_full, tidy_pci)
  )
  
  write_pci <- function(x,y) {
    d <- bind_rows(joined_pci_ranking[[x]])
    saveRDS(d, file = y, compress = "xz")
  }
  
  map2(seq_along(joined_pci_ranking), pci_files, write_pci)
  
  rm(joined_pci_ranking)
}

complexity()
