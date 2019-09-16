# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

compute_rca <- function(x, y, t, group_field) {
  if (file.exists(y[t])) {
    messageline()
    message(paste0("Skipping year ", years_full[t], ". The file already exist."))
  } else {
    messageline()
    message(paste0(
      "Creating smooth RCA file for the year ", years_full[t], ". Be patient..."
    ))
    
    trade_t1 <- fread2(x[t], character = "product_code", numeric = "trade_value_usd") %>%
      group_by(!!sym(group_field), product_code) %>%
      summarise(trade_value_usd_t1 = sum(trade_value_usd, na.rm = T)) %>%
      ungroup()
    
    if (years_full[t] <= years_missing_t_minus_1) {
      trade_t1 <- trade_t1 %>%
        mutate(trade_value_usd_t2 = NA)
    } else {
      trade_t2 <- fread2(x[t - 1], character = "product_code", numeric = "trade_value_usd") %>%
        group_by(!!sym(group_field), product_code) %>%
        summarise(trade_value_usd_t2 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
      
      trade_t1 <- trade_t1 %>%
        left_join(trade_t2, by = c(group_field, "product_code"))
    }
    
    if (years_full[t] <= years_missing_t_minus_2) {
      trade_t1 <- trade_t1 %>%
        mutate(trade_value_usd_t3 = NA)
    } else {
      trade_t3 <- fread2(x[t - 2], character = "product_code", numeric = "trade_value_usd") %>%
        group_by(!!sym(group_field), product_code) %>%
        summarise(trade_value_usd_t3 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
      
      trade_t1 <- trade_t1 %>%
        left_join(trade_t3, by = c(group_field, "product_code"))
    }
    
    trade_t1 <- trade_t1 %>%
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
      revealed_comparative_advantage(
        country = group_field,
        product = "product_code",
        value = "xcp",
        discrete = F,
        tbl_output = T
      ) %>%
      rename(!!sym(group_field) := country, product_code = product) %>%
      mutate(year = years_full[t]) %>%
      select(year, !!sym(group_field), product_code, value)
    
    if (group_field == "reporter_iso") {
      names(trade_t1) <- c("year", "country_iso", "product_code", "export_rca")
    } else {
      names(trade_t1) <- c("year", "country_iso", "product_code", "import_rca")
    }
    
    fwrite(trade_t1, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
  }
}

compute_complexity_measures <- function(x, yr, ye, yf, zr, ze, zf, q, w, t) {
  # RCA data ----
  
  rca_data <- fread2(x[t], character = c("product_code")) %>%
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
    fwrite(reflections$complexity_index_c, str_replace(yr[t], ".gz", ""))
    compress_gz(str_replace(yr[t], ".gz", ""))
  }
  
  if (!file.exists(ye[t])) {
    fwrite(eigenvalues$complexity_index_c, str_replace(ye[t], ".gz", ""))
    compress_gz(str_replace(ye[t], ".gz", ""))
  }
  
  if (!file.exists(yf[t])) {
    fwrite(fitness$complexity_index_c, str_replace(yf[t], ".gz", ""))
    compress_gz(str_replace(yf[t], ".gz", ""))
  }
  
  # save PCI ----
  
  if (!file.exists(zr[t])) {
    fwrite(
      reflections$complexity_index_p,
      str_replace(zr[t], ".gz", "")
    )
    compress_gz(str_replace(zr[t], ".gz", ""))
  }
  
  if (!file.exists(ze[t])) {
    fwrite(
      eigenvalues$complexity_index_p,
      str_replace(ze[t], ".gz", "")
    )
    compress_gz(str_replace(ze[t], ".gz", ""))
  }
  
  if (!file.exists(zf[t])) {
    fwrite(
      fitness$complexity_index_p,
      str_replace(zf[t], ".gz", "")
    )
    compress_gz(str_replace(zf[t], ".gz", ""))
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
    fwrite(proximity$proximity_c, str_replace(q[t], ".gz", ""))
    compress_gz(str_replace(q[t], ".gz", ""))
  }
  
  if (!file.exists(w[t])) {
    fwrite(proximity$proximity_p, str_replace(w[t], ".gz", ""))
    compress_gz(str_replace(w[t], ".gz", ""))
  }
}
