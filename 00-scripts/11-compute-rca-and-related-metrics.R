# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

compute_rca <- function(x, y, keep, discard, t) {
  if (file.exists(y[t])) {
    messageline()
    message(paste0("Skipping year ", years_full[t], ". The file already exist."))
  } else {
    messageline()
    message(paste0(
      "Creating smooth RCA file for the year ", years_full[t], ". Be patient..."
    ))
    
    trade_t1 <- fread2(x[t], char = c("commodity_code"), num = c("trade_value_usd")) %>%
      select(-!!sym(discard)) %>%
      group_by(year, !!sym(keep), commodity_code, commodity_code_length) %>%
      summarise(trade_value_usd_t1 = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup()
    
    trade_t1_4 <- filter(trade_t1, commodity_code_length == 4)
    trade_t1_6 <- filter(trade_t1, commodity_code_length == 6)
    rm(trade_t1)
    
    if (years_full[t] <= years_missing_t_minus_1) {
      trade_t2_4 <- trade_t1_4 %>%
        select(!!sym(keep), commodity_code) %>%
        mutate(trade_value_usd_t2 = NA)
      
      trade_t2_6 <- trade_t1_6 %>%
        select(!!sym(keep), commodity_code) %>%
        mutate(trade_value_usd_t2 = NA)
    } else {
      trade_t2 <- fread2(x[t - 1], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        select(-!!sym(discard)) %>%
        group_by(!!sym(keep), commodity_code, commodity_code_length) %>%
        summarise(trade_value_usd_t2 = sum(trade_value_usd, na.rm = T)) %>% 
        ungroup()
      
      trade_t2_4 <- trade_t2 %>% filter(commodity_code_length == 4) %>% select(-commodity_code_length)
      trade_t2_6 <- trade_t2 %>% filter(commodity_code_length == 6) %>% select(-commodity_code_length)
      rm(trade_t2)
    }
    
    if (years_full[t] <= years_missing_t_minus_2) {
      trade_t3_4 <- trade_t1_4 %>%
        select(!!sym(keep), commodity_code) %>%
        mutate(trade_value_usd_t3 = NA)
      
      trade_t3_6 <- trade_t1_6 %>%
        select(!!sym(keep), commodity_code) %>%
        mutate(trade_value_usd_t3 = NA)
    } else {
      trade_t3 <- fread2(x[t - 2], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        select(-!!sym(discard)) %>%
        group_by(!!sym(keep), commodity_code, commodity_code_length) %>%
        summarise(trade_value_usd_t3 = sum(trade_value_usd, na.rm = T)) %>% 
        ungroup()
      
      trade_t3_4 <- trade_t3 %>% filter(commodity_code_length == 4) %>% select(-commodity_code_length)
      trade_t3_6 <- trade_t3 %>% filter(commodity_code_length == 6) %>% select(-commodity_code_length)
      rm(trade_t3)
    }
    
    trade_t1_4 <- trade_t1_4 %>%
      left_join(trade_t2_4, by = c(keep, "commodity_code")) %>%
      left_join(trade_t3_4, by = c(keep, "commodity_code")) %>%
      
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
      
      group_by(commodity_code) %>% # Sum by product
      mutate(sum_p_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      group_by(!!sym(keep)) %>% # Sum by country
      mutate(sum_c_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      mutate(
        sum_c_p_xcp = sum(xcp, na.rm = TRUE), # World's total exported value
        rca = (xcp / sum_c_xcp) / (sum_p_xcp / sum_c_p_xcp) # Compute RCA
      ) %>%
      
      select(year, !!sym(keep), commodity_code, rca)
    
    trade_t1_6 <- trade_t1_6 %>%
      left_join(trade_t2_6, by = c(keep, "commodity_code")) %>%
      left_join(trade_t3_6, by = c(keep, "commodity_code")) %>%
      
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
      
      group_by(commodity_code) %>% # Sum by product
      mutate(sum_p_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      group_by(!!sym(keep)) %>% # Sum by country
      mutate(sum_c_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      
      mutate(
        sum_c_p_xcp = sum(xcp, na.rm = TRUE), # World's total exported value
        rca = (xcp / sum_c_xcp) / (sum_p_xcp / sum_c_p_xcp) # Compute RCA
      ) %>%
      
      select(year, !!sym(keep), commodity_code, rca)
    
    trade_t1 <- bind_rows(trade_t1_4, trade_t1_6) %>% arrange(!!sym(keep), commodity_code)
    rm(trade_t1_4, trade_t1_6)
    
    if (keep == "reporter_iso") {
      names(trade_t1) <- c("year", "country_iso", "commodity_code", "export_rca")
    } else {
      names(trade_t1) <- c("year", "country_iso", "commodity_code", "import_rca")
    }
    
    fwrite(trade_t1, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
  }
}

compute_rca_exports <- function() {
  lapply(seq_along(years_full), compute_rca,
         x = unified_gz, y = rca_exports_gz,
         keep = "reporter_iso", discard = "partner_iso"
  ) 
}

compute_rca_imports <- function() {
  lapply(seq_along(years_full), compute_rca,
         x = unified_gz, y = rca_imports_gz,
         keep = "partner_iso", discard = "reporter_iso"
  )
}

compute_rca_metrics <- function(x, y, z, q, w, e, r, t) {
  if (!file.exists(str_replace(r[t], ".gz", ""))) {
    # RCA matrix (Mcp) ---------------------------------------------------------
    
    rca_tibble <- fread2(x[t], char = c("commodity_code")) %>%
      select(-year) %>%
      filter(str_length(commodity_code) == 4) %>% 
      inner_join(select(ranking_1, reporter_iso), by = c("country_iso" = "reporter_iso")) %>%
      mutate(export_rca = ifelse(export_rca > 1, 1, 0)) %>%
      spread(commodity_code, export_rca)
    
    diversity <- rca_tibble %>% select(country_iso)
    ubiquity <- tibble(product = colnames(rca_tibble)) %>% filter(row_number() > 1)
    
    Mcp <- rca_tibble %>%
      select(-country_iso) %>%
      as.matrix()
    
    # convert to sparse class
    Mcp[is.na(Mcp)] <- 0
    Mcp <- Matrix(Mcp, sparse = T)
    
    diversity <- diversity %>%
      mutate(val = rowSums(Mcp, na.rm = TRUE))

    ubiquity <- ubiquity %>%
      mutate(val = colSums(Mcp, na.rm = TRUE))

    rownames(Mcp) <- diversity$country_iso
    
    # remove null rows and cols
    Mcp <- Mcp[rowSums(Mcp, na.rm = TRUE) != 0, colSums(Mcp, na.rm = TRUE) != 0]
    
    diversity <- filter(diversity, country_iso %in% rownames(Mcp))
    ubiquity <- filter(ubiquity, product %in% colnames(Mcp))
    
    D <- as.matrix(diversity$val, ncol = 1)
    U <- as.matrix(ubiquity$val, ncol = 1)
    
    rm(rca_tibble)
    
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
    kc[, 1] <- kc0
    kp[, 1] <- kp0
    
    # compute cols 2 to 20 by iterating from col 1
    for (c in 2:ncol(kc)) {
      kc[, c] <- kcinv * (Mcp %*% kp[, (c - 1)])
      kp[, c] <- kpinv * (t(Mcp) %*% kc[, (c - 1)])
    }
    
    # ECI (reflections method) ------------------------------------------------
    
    eci_reflections <- as_tibble(
        (kc[, 19] - mean(kc[, 19])) / sd(kc[, 19])
      ) %>%
      mutate(
        country_iso = diversity$country_iso,
        year = years_full[t]
      ) %>%
      select(year, country_iso, value) %>%
      arrange(desc(value)) %>%
      rename(eci = value)
    
    fwrite(eci_reflections, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
    
    # PCI (reflections method) ------------------------------------------------
    
    pci_reflections <- as_tibble(
        (kp[, 20] - mean(kp[, 20])) / sd(kp[, 20])
      ) %>%
      mutate(
        commodity_code = ubiquity$product,
        year = years_full[t]
      ) %>%
      select(year, commodity_code, value) %>%
      arrange(desc(value)) %>%
      rename(pci = value)
    
    fwrite(pci_reflections, str_replace(z[t], ".gz", ""))
    compress_gz(str_replace(z[t], ".gz", ""))
    
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
    
    Phi_cc <- (Mcp %*% t(Mcp)) / proximity_countries_denominator(Mcp, D, cores = min(1,n_cores))
    
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
    
    Phi_pp <- (t(Mcp) %*% Mcp) / proximity_products_denominator(Mcp, U, cores = min(1,n_cores))
    
    Phi_pp_l <- Phi_pp
    Phi_pp_l[upper.tri(Phi_pp_l, diag = T)] <- NA
    
    Phi_pp_long <- as_tibble(as.matrix(Phi_pp_l)) %>%
      mutate(id = rownames(Phi_pp)) %>%
      gather(id2, value, -id) %>%
      filter(!is.na(value)) %>%
      setNames(c("commodity_code", "commodity_code_2", "value"))
    
    fwrite(Phi_pp_long, str_replace(w[t], ".gz", ""))
    compress_gz(str_replace(w[t], ".gz", ""))
    rm(Phi_pp_l, Phi_pp_long)
    
    # density (countries) -----------------------------------------------------
    
    Omega_countries_cp <- (Phi_cc %*% Mcp) / colSums(Phi_cc)
    
    Omega_countries_cp_long <- as_tibble(as.matrix(Omega_countries_cp)) %>%
      mutate(country_iso = rownames(Omega_countries_cp)) %>%
      gather(product, value, -country_iso) %>%
      setNames(c("country_iso", "commodity_code", "value"))
    
    fwrite(Omega_countries_cp_long, str_replace(e[t], ".gz", ""))
    compress_gz(str_replace(e[t], ".gz", ""))
    rm(Omega_countries_cp, Omega_countries_cp_long)
    
    # density (products) ------------------------------------------------------
    
    Omega_products_cp <- t((Phi_pp %*% t(Mcp)) / colSums(Phi_pp))
    
    Omega_products_cp_long <- as_tibble(as.matrix(Omega_products_cp)) %>%
      mutate(country_iso = rownames(Omega_products_cp)) %>%
      gather(product, value, -country_iso) %>%
      setNames(c("country_iso", "commodity_code", "value"))
    
    fwrite(Omega_products_cp_long, str_replace(r[t], ".gz", ""))
    compress_gz(str_replace(r[t], ".gz", ""))
  }
}
