# Open ts-yearly-data.Rproj before running this function

compute_rca <- function(x, y, z, keep, discard, t) {
  if (file.exists(z[[t]])) {
    messageline()
    message(paste0("Skipping year ", years[[t]], ". The file already exist."))
  } else {
    messageline()
    message(paste0(
      "Creating smooth RCA file for the year ", years[[t]], ". Be patient..."
    ))
    
    trade_t1 <- fread3(x[[t]]) %>%
      select(-!!sym(discard)) %>%
      unite(pairs, year, !!sym(keep), commodity_code, remove = TRUE) %>%
      group_by(pairs) %>%
      summarise(trade_value_usd_t1 = sum(trade_value_usd, na.rm = T)) %>%
      ungroup()
    
    if (years[[t]] <= years_missing_t_minus_1) {
      trade_t2 <- trade_t1 %>%
        select(pairs) %>%
        mutate(trade_value_usd_t2 = NA)
    } else {
      trade_t2 <- fread3(x[[t - 1]]) %>%
        select(-!!sym(discard)) %>%
        unite(pairs, year, !!sym(keep), commodity_code, remove = TRUE) %>%
        group_by(pairs) %>%
        summarise(trade_value_usd_t2 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
    }
    
    if (years[[t]] <= years_missing_t_minus_2) {
      trade_t3 <- trade_t1 %>%
        select(pairs) %>%
        mutate(trade_value_usd_t3 = NA)
    } else {
      trade_t3 <- fread3(x[[t - 2]]) %>%
        select(-!!sym(discard)) %>%
        unite(pairs, year, !!sym(keep), commodity_code, remove = FALSE) %>%
        group_by(pairs) %>%
        summarise(trade_value_usd_t3 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
    }
    
    trade_t1 <- trade_t1 %>%
      left_join(trade_t2, by = "pairs") %>%
      left_join(trade_t3, by = "pairs") %>%
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
      separate(pairs, c("year", keep, "commodity_code")) %>%
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
    
    if (keep == "reporter_iso") {
      names(trade_t1) <- c("year", "country_iso", "commodity_code", "export_rca")
    } else {
      names(trade_t1) <- c("year", "country_iso", "commodity_code", "import_rca")
    }
    
    fwrite(trade_t1, y[[t]])
    compress_gz(y[[t]])
  }
}

compute_rca_exports <- function() {
  lapply(seq_along(years), compute_rca,
         x = unified_gz, y = rca_exports_csv, z = rca_exports_gz,
         keep = "reporter_iso", discard = "partner_iso"
  ) 
}

compute_rca_imports <- function() {
  lapply(seq_along(years), compute_rca,
         x = unified_gz, y = rca_imports_csv, z = rca_imports_gz,
         keep = "partner_iso", discard = "reporter_iso"
  )
}

compute_rca_metrics <- function(x, y, z, w, q, r, s, t) {
  if (!file.exists(gsub("csv", "csv.gz", s[[t]]))) {
    # RCA matrix --------------------------------------------------------------
    
    rca_matrix <- fread4(x[[t]]) %>%
      select(-year) %>%
      inner_join(select(ranking_1, reporter_iso), by = c("country_iso" = "reporter_iso")) %>%
      mutate(export_rca = ifelse(export_rca > 1, 1, 0)) %>%
      spread(commodity_code, export_rca)
    
    diversity <- rca_matrix %>% select(country_iso)
    ubiquity <- tibble(product = colnames(rca_matrix)) %>% filter(row_number() > 1)
    
    rca_matrix <- rca_matrix %>%
      select(-country_iso) %>%
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
    
    rownames(rca_matrix) <- diversity$country_iso
    
    D <- as.matrix(diversity$val, ncol = 1)
    U <- as.matrix(ubiquity$val, ncol = 1)
    
    # remove null rows and cols
    Mcp <- rca_matrix[
      which(rownames(rca_matrix) %in% unlist(diversity$country_iso)),
      which(colnames(rca_matrix) %in% unlist(ubiquity$product))
      ]
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
      mutate(country_iso = diversity$country_iso) %>%
      mutate(year = years[[t]]) %>%
      select(year, country_iso, value) %>%
      arrange(desc(value)) %>%
      rename(eci = value)
    
    fwrite(eci_reflections, y[[t]])
    compress_gz(y[[t]])
    
    # PCI (reflections method) ------------------------------------------------
    
    pci_reflections <- as_tibble(
      (kp[, 20] - mean(kp[, 20])) / sd(kp[, 20])
    ) %>%
      mutate(commodity_code = ubiquity$product) %>%
      mutate(year = years[[t]]) %>%
      select(year, commodity_code, value) %>%
      arrange(desc(value)) %>%
      rename(pci = value)
    
    fwrite(pci_reflections, z[[t]])
    compress_gz(z[[t]])
    
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
    
    fwrite(Phi_cc_long, w[[t]])
    compress_gz(w[[t]])
    rm(Phi_cc_l, Phi_cc_long)
    
    # proximity (products) ---------------------------------------------------
    
    Phi_pp <- (t(Mcp) %*% Mcp) / proximity_products_denominator(Mcp, U, cores = min(1,n_cores))
    
    Phi_pp_l <- Phi_pp
    Phi_pp_l[upper.tri(Phi_pp_l, diag = T)] <- NA
    
    Phi_pp_long <- as_tibble(as.matrix(Phi_pp_l)) %>%
      mutate(id = rownames(Phi_pp)) %>%
      gather(id2, value, -id) %>%
      filter(!is.na(value)) %>%
      setNames(c("product_hs07_id", "product_hs07_id_2", "value"))
    
    fwrite(Phi_pp_long, q[[t]])
    compress_gz(q[[t]])
    rm(Phi_pp_l, Phi_pp_long)
    
    # density (countries) -----------------------------------------------------
    
    Omega_countries_cp <- (Phi_cc %*% Mcp) / colSums(Phi_cc)
    
    Omega_countries_cp_long <- as_tibble(as.matrix(Omega_countries_cp)) %>%
      mutate(country_iso = rownames(Omega_countries_cp)) %>%
      gather(product, value, -country_iso) %>%
      setNames(c("country_iso", "product_hs07_id", "value"))
    
    fwrite(Omega_countries_cp_long, r[[t]])
    compress_gz(r[[t]])
    rm(Omega_countries_cp, Omega_countries_cp_long)
    
    # density (products) ------------------------------------------------------
    
    Omega_products_cp <- t((Phi_pp %*% t(Mcp)) / colSums(Phi_pp))
    
    Omega_products_cp_long <- as_tibble(as.matrix(Omega_products_cp)) %>%
      mutate(country_iso = rownames(Omega_products_cp)) %>%
      gather(product, value, -country_iso) %>%
      setNames(c("country_iso", "product_hs07_id", "value"))
    
    fwrite(Omega_products_cp_long, s[[t]])
    compress_gz(s[[t]])
  }
}
