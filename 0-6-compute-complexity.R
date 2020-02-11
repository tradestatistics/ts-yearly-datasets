# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

complexity <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
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
    
    if (!file.exists(yr[t])) {
      reflections <- ec_complexity_measures(
        rca = rca_data,
        method = "reflections",
        tbl = TRUE
      )
    }
    
    if (!file.exists(ye[t])) {
      eigenvalues <- ec_complexity_measures(
        rca = rca_data,
        method = "eigenvalues",
        tbl = TRUE
      )
    }
    
    if (!file.exists(yf[t])) {
      fitness <- ec_complexity_measures(
        rca = rca_data,
        method = "fitness",
        tbl = TRUE
      )
    }
    
    # save ECI ----
    
    if (!file.exists(yr[t])) {
      saveRDS(reflections$complexity_index_c, file = yr[t], compress = "xz")
    }
    
    if (!file.exists(ye[t])) {
      saveRDS(eigenvalues$complexity_index_c, file = ye[t], compress = "xz")
    }
    
    if (!file.exists(yf[t])) {
      saveRDS(fitness$complexity_index_c, file = yf[t], compress = "xz")
    }
    
    # save PCI ----
    
    if (!file.exists(zr[t])) {
      saveRDS(reflections$complexity_index_p, file = zr[t], compress = "xz")
    }
    
    if (!file.exists(ze[t])) {
      saveRDS(eigenvalues$complexity_index_p, file = ze[t], compress = "xz")
    }
    
    if (!file.exists(zf[t])) {
      saveRDS(fitness$complexity_index_p, file = zf[t], compress = "xz")
    }
    
    # proximity ----
    
    if (!file.exists(q[t]) | !file.exists(w[t])) {
      proximity <- ec_proximity(
        rca = rca_data,
        d = fitness$diversity,
        u = fitness$ubiquity,
        tbl = TRUE
      )
    }
    
    if (!file.exists(q[t])) {
      saveRDS(proximity$proximity_c, file = q[t], compress = "xz")
    }
    
    if (!file.exists(w[t])) {
      saveRDS(proximity$proximity_p, file = w[t], compress = "xz")
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
      arrange(-value) %>%
      rename(eci = value) %>%
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
      arrange(-value) %>%
      rename(pci = value) %>%
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
