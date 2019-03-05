# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

compute_rca <- function(x, y, keep, t) {
  if (file.exists(y[t])) {
    messageline()
    message(paste0("Skipping year ", years_full[t], ". The file already exist."))
  } else {
    messageline()
    message(paste0(
      "Creating smooth RCA file for the year ", years_full[t], ". Be patient..."
    ))
    
    trade_t1 <- fread2(x[t], character = "product_code", numeric = "trade_value_usd") %>%
      group_by(year, !!sym(keep), product_code, product_code_length) %>%
      summarise(trade_value_usd_t1 = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup()
    
    trade_t1_4 <- filter(trade_t1, product_code_length == 4)
    trade_t1_6 <- filter(trade_t1, product_code_length == 6)
    rm(trade_t1)
    
    if (years_full[t] <= years_missing_t_minus_1) {
      trade_t1_4 <- trade_t1_4 %>%
        mutate(trade_value_usd_t2 = NA)
      
      trade_t1_6 <- trade_t1_6 %>%
        mutate(trade_value_usd_t2 = NA)
    } else {
      trade_t2 <- fread2(x[t - 1], character = "product_code", numeric = "trade_value_usd") %>%
        group_by(!!sym(keep), product_code, product_code_length) %>%
        summarise(trade_value_usd_t2 = sum(trade_value_usd, na.rm = T)) %>% 
        ungroup()
      
      trade_t2_4 <- trade_t2 %>% filter(product_code_length == 4)
      trade_t2_6 <- trade_t2 %>% filter(product_code_length == 6)
      rm(trade_t2)
      
      trade_t1_4 <- trade_t1_4 %>%
        left_join(trade_t2_4, by = c(keep, "product_code"))
      
      trade_t1_6 <- trade_t1_6 %>%
        left_join(trade_t2_6, by = c(keep, "product_code"))
    }
    
    if (years_full[t] <= years_missing_t_minus_2) {
      trade_t1_4 <- trade_t1_4 %>%
        mutate(trade_value_usd_t3 = NA)
      
      trade_t1_6 <- trade_t1_6 %>%
        mutate(trade_value_usd_t3 = NA)
    } else {
      trade_t3 <- fread2(x[t - 2], character = "product_code", numeric = "trade_value_usd") %>%
        group_by(!!sym(keep), product_code, product_code_length) %>%
        summarise(trade_value_usd_t3 = sum(trade_value_usd, na.rm = T)) %>% 
        ungroup()
      
      trade_t3_4 <- trade_t3 %>% filter(product_code_length == 4)
      trade_t3_6 <- trade_t3 %>% filter(product_code_length == 6)
      rm(trade_t3)
      
      trade_t1_4 <- trade_t1_4 %>%
        left_join(trade_t3_4, by = c(keep, "product_code"))
      
      trade_t1_6 <- trade_t1_6 %>%
        left_join(trade_t3_6, by = c(keep, "product_code"))
    }
    
    trade_t1_4 <- trade_t1_4 %>%
      rowwise() %>% # To apply a weighted mean by rows with 1 weight = 1 column
      mutate(
        xcp = weighted.mean( # x = value, c = country, p = product
          x = c(trade_value_usd_t1, trade_value_usd_t3, trade_value_usd_t3),
          w = c(2, 1, 1),
          na.rm = TRUE
        )
      ) %>%
      ungroup() %>%
      select(-c(trade_value_usd_t1, trade_value_usd_t2, trade_value_usd_t3)) %>%
      
      group_by(product_code) %>% # Sum by product
      mutate(sum_p_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      group_by(!!sym(keep)) %>% # Sum by country
      mutate(sum_c_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      mutate(
        sum_c_p_xcp = sum(xcp, na.rm = TRUE), # World's total exported value
        rca = (xcp / sum_c_xcp) / (sum_p_xcp / sum_c_p_xcp) # Compute RCA
      ) %>%
      
      select(year, !!sym(keep), product_code, rca)
    
    trade_t1_6 <- trade_t1_6 %>%
      rowwise() %>% # To apply a weighted mean by rows with 1 weight = 1 column
      mutate(
        xcp = weighted.mean( # x = value, c = country, p = product
          x = c(trade_value_usd_t1, trade_value_usd_t3, trade_value_usd_t3),
          w = c(2, 1, 1),
          na.rm = TRUE
        )
      ) %>%
      ungroup() %>%
      select(-c(trade_value_usd_t1, trade_value_usd_t2, trade_value_usd_t3)) %>%
      
      group_by(product_code) %>% # Sum by product
      mutate(sum_p_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      group_by(!!sym(keep)) %>% # Sum by country
      mutate(sum_c_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      mutate(
        sum_c_p_xcp = sum(xcp, na.rm = TRUE), # World's total exported value
        rca = (xcp / sum_c_xcp) / (sum_p_xcp / sum_c_p_xcp) # Compute RCA
      ) %>%
      
      select(year, !!sym(keep), product_code, rca)
    
    trade_t1 <- bind_rows(trade_t1_4, trade_t1_6) %>% arrange(!!sym(keep), product_code)
    rm(trade_t1_4, trade_t1_6)
    
    if (keep == "reporter_iso") {
      names(trade_t1) <- c("year", "country_iso", "product_code", "export_rca")
    } else {
      names(trade_t1) <- c("year", "country_iso", "product_code", "import_rca")
    }
    
    fwrite(trade_t1, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
  }
}

compute_rca_metrics <- function(x, y, z, q, w, n_cores, t) {
  if (!file.exists(w[t])) {
    # RCA matrix (Mcp) ---------------------------------------------------------
    
    rca_data <- fread2(x[t], character = c("product_code"))
    
    rca_tibble_4 <- rca_data %>%
      select(-year) %>%
      filter(str_length(product_code) == 4) %>% 
      inner_join(select(ranking_1, reporter_iso), by = c("country_iso" = "reporter_iso")) %>%
      mutate(export_rca = ifelse(export_rca > 1, 1, 0)) %>%
      spread(product_code, export_rca)
    
    rca_tibble_6 <- rca_data %>%
      select(-year) %>%
      filter(str_length(product_code) == 6) %>% 
      inner_join(select(ranking_1, reporter_iso), by = c("country_iso" = "reporter_iso")) %>%
      mutate(export_rca = ifelse(export_rca > 1, 1, 0)) %>%
      spread(product_code, export_rca)
    
    diversity_4 <- rca_tibble_4 %>% select(country_iso)
    diversity_6 <- rca_tibble_6 %>% select(country_iso)
    
    ubiquity_4 <- tibble(product = colnames(rca_tibble_4)) %>% filter(row_number() > 1)
    ubiquity_6 <- tibble(product = colnames(rca_tibble_6)) %>% filter(row_number() > 1)
    
    Mcp_4 <- rca_tibble_4 %>%
      select(-country_iso) %>%
      as.matrix()
    
    Mcp_6 <- rca_tibble_6 %>%
      select(-country_iso) %>%
      as.matrix()
    
    # convert to sparse class
    Mcp_4[is.na(Mcp_4)] <- 0
    Mcp_4 <- Matrix(Mcp_4, sparse = T)
    
    Mcp_6[is.na(Mcp_6)] <- 0
    Mcp_6 <- Matrix(Mcp_6, sparse = T)
    
    diversity_4 <- diversity_4 %>% mutate(val = rowSums(Mcp_4, na.rm = TRUE))
    diversity_6 <- diversity_6 %>% mutate(val = rowSums(Mcp_6, na.rm = TRUE))
    
    ubiquity_4 <- ubiquity_4 %>% mutate(val = colSums(Mcp_4, na.rm = TRUE))
    ubiquity_6 <- ubiquity_6 %>% mutate(val = colSums(Mcp_6, na.rm = TRUE))
    
    rownames(Mcp_4) <- diversity_4$country_iso
    rownames(Mcp_6) <- diversity_6$country_iso
    
    # remove null rows and cols
    Mcp_4 <- Mcp_4[rowSums(Mcp_4, na.rm = TRUE) != 0, colSums(Mcp_4, na.rm = TRUE) != 0]
    Mcp_6 <- Mcp_6[rowSums(Mcp_6, na.rm = TRUE) != 0, colSums(Mcp_6, na.rm = TRUE) != 0]
    
    diversity_4 <- filter(diversity_4, country_iso %in% rownames(Mcp_4))
    diversity_6 <- filter(diversity_6, country_iso %in% rownames(Mcp_6))
    
    ubiquity_4 <- filter(ubiquity_4, product %in% colnames(Mcp_4))
    ubiquity_6 <- filter(ubiquity_6, product %in% colnames(Mcp_6))
    
    D_4 <- as.matrix(diversity_4$val, ncol = 1)
    D_6 <- as.matrix(diversity_6$val, ncol = 1)
    
    U_4 <- as.matrix(ubiquity_4$val, ncol = 1)
    U_6 <- as.matrix(ubiquity_6$val, ncol = 1)
    
    rm(rca_data, rca_tibble_4, rca_tibble_6)
    
    # diversity and ubiquity following the Atlas notation
    kc0_4 <- as.numeric(D_4)
    kp0_4 <- as.numeric(U_4)
    
    kc0_6 <- as.numeric(D_6)
    kp0_6 <- as.numeric(U_6)
    
    # reflections method ------------------------------------------------------
    
    kcinv_4 <- 1 / kc0_4
    kpinv_4 <- 1 / kp0_4
    
    kcinv_6 <- 1 / kc0_6
    kpinv_6 <- 1 / kp0_6
    
    # create empty matrices
    kc_4 <- Matrix(0, nrow = length(kc0_4), ncol = 20, sparse = T)
    kp_4 <- Matrix(0, nrow = length(kp0_4), ncol = 20, sparse = T)
    
    kc_6 <- Matrix(0, nrow = length(kc0_6), ncol = 20, sparse = T)
    kp_6 <- Matrix(0, nrow = length(kp0_6), ncol = 20, sparse = T)
    
    # fill the first column with kc0 and kp0 to start iterating
    kc_4[, 1] <- kc0_4
    kp_4[, 1] <- kp0_4
    
    kc_6[, 1] <- kc0_6
    kp_6[, 1] <- kp0_6
    
    # compute cols 2 to 20 by iterating from col 1
    for (c in 2:ncol(kc_4)) {
      kc_4[, c] <- kcinv_4 * (Mcp_4 %*% kp_4[, (c - 1)])
      kp_4[, c] <- kpinv_4 * (t(Mcp_4) %*% kc_4[, (c - 1)])
    }
    
    for (c in 2:ncol(kc_6)) {
      kc_6[, c] <- kcinv_6 * (Mcp_6 %*% kp_6[, (c - 1)])
      kp_6[, c] <- kpinv_6 * (t(Mcp_6) %*% kc_6[, (c - 1)])
    }
    
    # ECI (reflections method) ------------------------------------------------
    
    eci_reflections <- as_tibble(
        (kc_4[, 19] - mean(kc_4[, 19])) / sd(kc_4[, 19])
      ) %>%
      mutate(
        country_iso = diversity_4$country_iso,
        year = years_full[t]
      ) %>%
      select(year, country_iso, value) %>%
      rename(eci = value)
    
    fwrite(eci_reflections, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
    
    # PCI (reflections method) ------------------------------------------------
    
    pci_reflections_4 <- as_tibble(
        (kp_4[, 20] - mean(kp_4[, 20])) / sd(kp_4[, 20])
      ) %>%
      mutate(
        product_code = ubiquity_4$product,
        year = years_full[t]
      ) %>%
      select(year, product_code, value) %>%
      rename(pci = value)
    
    pci_reflections_6 <- as_tibble(
      (kp_6[, 20] - mean(kp_6[, 20])) / sd(kp_6[, 20])
    ) %>%
      mutate(
        product_code = ubiquity_6$product,
        year = years_full[t]
      ) %>%
      select(year, product_code, value) %>%
      rename(pci = value)
    
    pci_reflections <- bind_rows(pci_reflections_4, pci_reflections_6) %>% arrange(product_code)
    
    fwrite(pci_reflections, str_replace(z[t], ".gz", ""))
    compress_gz(str_replace(z[t], ".gz", ""))
    
    rm(
      kc0_4,
      kp0_4,
      kcinv_4,
      kpinv_4,
      kc_4,
      kp_4,
      eci_reflections,
      pci_reflections_4,
      pci_reflections_6,
      kc0_6,
      kp0_6,
      kcinv_6,
      kpinv_6,
      kc_6,
      kp_6,
      c
    )
    
    # proximity (countries) ---------------------------------------------------
    
    Phi_cc <- (Mcp_4 %*% t(Mcp_4)) / proximity_countries_denominator(Mcp_4, D_4, cores = min(1,n_cores))
    
    Phi_cc_l <- Phi_cc
    Phi_cc_l[upper.tri(Phi_cc_l, diag = T)] <- NA
    
    Phi_cc_long <- as_tibble(as.matrix(Phi_cc_l)) %>%
      mutate(id = rownames(Phi_cc)) %>%
      gather(id2, value, -id) %>%
      filter(!is.na(value)) %>%
      setNames(c("country_iso", "country_iso_2", "value"))
    
    fwrite(Phi_cc_long, str_replace(q[t], ".gz", ""))
    compress_gz(str_replace(q[t], ".gz", ""))
    rm(Phi_cc_l, Phi_cc_long)
    
    # proximity (products) ---------------------------------------------------
    
    Phi_pp <- (t(Mcp_4) %*% Mcp_4) / proximity_products_denominator(Mcp_4, U_4, cores = min(1,n_cores))
    
    Phi_pp_l <- Phi_pp
    Phi_pp_l[upper.tri(Phi_pp_l, diag = T)] <- NA
    
    Phi_pp_long <- as_tibble(as.matrix(Phi_pp_l)) %>%
      mutate(id = rownames(Phi_pp)) %>%
      gather(id2, value, -id) %>%
      filter(!is.na(value)) %>%
      setNames(c("product_code", "product_code_2", "value"))
    
    fwrite(Phi_pp_long, str_replace(w[t], ".gz", ""))
    compress_gz(str_replace(w[t], ".gz", ""))
    rm(Phi_pp_l, Phi_pp_long)
    
    # density (countries) -----------------------------------------------------
    
    # Omega_countries_cp <- (Phi_cc %*% Mcp) / colSums(Phi_cc)
    # 
    # Omega_countries_cp_long <- as_tibble(as.matrix(Omega_countries_cp)) %>%
    #   mutate(country_iso = rownames(Omega_countries_cp)) %>%
    #   gather(product, value, -country_iso) %>%
    #   setNames(c("country_iso", "product_code", "value"))
    # 
    # fwrite(Omega_countries_cp_long, str_replace(e[t], ".gz", ""))
    # compress_gz(str_replace(e[t], ".gz", ""))
    # rm(Omega_countries_cp, Omega_countries_cp_long)
    
    # density (products) ------------------------------------------------------
    
    # Omega_products_cp <- t((Phi_pp %*% t(Mcp)) / colSums(Phi_pp))
    # 
    # Omega_products_cp_long <- as_tibble(as.matrix(Omega_products_cp)) %>%
    #   mutate(country_iso = rownames(Omega_products_cp)) %>%
    #   gather(product, value, -country_iso) %>%
    #   setNames(c("country_iso", "product_code", "value"))
    # 
    # fwrite(Omega_products_cp_long, str_replace(r[t], ".gz", ""))
    # compress_gz(str_replace(r[t], ".gz", ""))
  }
}
