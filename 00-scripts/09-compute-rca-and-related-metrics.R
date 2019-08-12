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
      group_by(year, !!sym(group_field), product_code, product_code_length) %>%
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
        group_by(!!sym(group_field), product_code, product_code_length) %>%
        summarise(trade_value_usd_t2 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
      
      trade_t2_4 <- trade_t2 %>% filter(product_code_length == 4)
      trade_t2_6 <- trade_t2 %>% filter(product_code_length == 6)
      rm(trade_t2)
      
      trade_t1_4 <- trade_t1_4 %>%
        left_join(trade_t2_4, by = c(group_field, "product_code"))
      
      trade_t1_6 <- trade_t1_6 %>%
        left_join(trade_t2_6, by = c(group_field, "product_code"))
    }
    
    if (years_full[t] <= years_missing_t_minus_2) {
      trade_t1_4 <- trade_t1_4 %>%
        mutate(trade_value_usd_t3 = NA)
      
      trade_t1_6 <- trade_t1_6 %>%
        mutate(trade_value_usd_t3 = NA)
    } else {
      trade_t3 <- fread2(x[t - 2], character = "product_code", numeric = "trade_value_usd") %>%
        group_by(!!sym(group_field), product_code, product_code_length) %>%
        summarise(trade_value_usd_t3 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
      
      trade_t3_4 <- trade_t3 %>% filter(product_code_length == 4)
      trade_t3_6 <- trade_t3 %>% filter(product_code_length == 6)
      rm(trade_t3)
      
      trade_t1_4 <- trade_t1_4 %>%
        left_join(trade_t3_4, by = c(group_field, "product_code"))
      
      trade_t1_6 <- trade_t1_6 %>%
        left_join(trade_t3_6, by = c(group_field, "product_code"))
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
    
    trade_t1 <- bind_rows(trade_t1_4, trade_t1_6) %>% arrange(!!sym(group_field), product_code)
    rm(trade_t1_4, trade_t1_6)
    
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
  
  rca_4 <- rca_data %>%
    filter(str_length(product_code) == 4)
  
  if (compute_pci6 == 1) {
    rca_6 <- rca_data %>%
      filter(str_length(product_code) == 6)
  }
  
  rm(rca_data)
  
  # ECI/PCI 4 digits ----
  
  if (!file.exists(yr[t])) {
    reflections_4 <- complexity_measures(
      revealed_comparative_advantage = rca_4,
      country = "country_iso",
      product = "product_code",
      method = "reflections",
      value = "export_rca",
      tbl_output = TRUE
    )
  }
  
  if (!file.exists(ye[t])) {
    eigenvalues_4 <- complexity_measures(
      revealed_comparative_advantage = rca_4,
      country = "country_iso",
      product = "product_code",
      method = "eigenvalues",
      value = "export_rca",
      tbl_output = TRUE
    )
  }
  
  if (!file.exists(yf[t])) {
    fitness_4 <- complexity_measures(
      revealed_comparative_advantage = rca_4,
      country = "country_iso",
      product = "product_code",
      method = "fitness",
      value = "export_rca",
      tbl_output = TRUE
    )
  }
  
  # ECI/PCI 6 digits ----
  
  if (compute_pci6 == 1) {
    if (!file.exists(yr[t])) {
      reflections_6 <- complexity_measures(
        revealed_comparative_advantage = rca_6,
        country = "country_iso",
        product = "product_code",
        method = "reflections",
        value = "export_rca",
        tbl_output = TRUE
      )
    }
    
    if (!file.exists(ye[t])) {
      eigenvalues_6 <- complexity_measures(
        revealed_comparative_advantage = rca_6,
        country = "country_iso",
        product = "product_code",
        method = "eigenvalues",
        value = "export_rca",
        tbl_output = TRUE
      )
    }
    
    if (!file.exists(yf[t])) {
      fitness_6 <- complexity_measures(
        revealed_comparative_advantage = rca_6,
        country = "country_iso",
        product = "product_code",
        method = "fitness",
        value = "export_rca",
        tbl_output = TRUE
      )
    }
  }
  
  # save ECI ----
  
  if (!file.exists(yr[t])) {
    fwrite(reflections_4$economic_complexity_index, str_replace(yr[t], ".gz", ""))
    compress_gz(str_replace(yr[t], ".gz", ""))
  }
  
  if (!file.exists(ye[t])) {
    fwrite(eigenvalues_4$economic_complexity_index, str_replace(ye[t], ".gz", ""))
    compress_gz(str_replace(ye[t], ".gz", ""))
  }
  
  if (!file.exists(yf[t])) {
    fwrite(fitness_4$economic_complexity_index, str_replace(yf[t], ".gz", ""))
    compress_gz(str_replace(yf[t], ".gz", ""))
  }
  
  # save PCI 4/6 digits ----
  
  if (compute_pci6 == 1) {
    if (!file.exists(zr[t])) {
      fwrite(
        bind_rows(
          reflections_4$product_complexity_index,
          reflections_6$product_complexity_index
        ),
        str_replace(zr[t], ".gz", "")
      )
      compress_gz(str_replace(zr[t], ".gz", ""))
    }
    
    if (!file.exists(ze[t])) {
      fwrite(
        bind_rows(
          eigenvalues_4$product_complexity_index,
          eigenvalues_6$product_complexity_index
        ),
        str_replace(ze[t], ".gz", "")
      )
      compress_gz(str_replace(ze[t], ".gz", ""))
    }
    
    if (!file.exists(zf[t])) {
      fwrite(
        bind_rows(
          fitness_4$product_complexity_index,
          fitness_6$product_complexity_index
        ),
        str_replace(zf[t], ".gz", "")
      )
      compress_gz(str_replace(zf[t], ".gz", ""))
    }
    
    rm(
      fitness_4, fitness_6,
      reflections_4, reflections_6,
      eigenvalues_4, eigenvalues_6
    )
  } else {
    if (!file.exists(zr[t])) {
      fwrite(
        reflections_4$product_complexity_index,
        str_replace(zr[t], ".gz", "")
      )
      compress_gz(str_replace(zr[t], ".gz", ""))
    }
    
    if (!file.exists(ze[t])) {
      fwrite(
        eigenvalues_4$product_complexity_index,
        str_replace(ze[t], ".gz", "")
      )
      compress_gz(str_replace(ze[t], ".gz", ""))
    }
    
    if (!file.exists(zf[t])) {
      fwrite(
        fitness_4$product_complexity_index,
        str_replace(zf[t], ".gz", "")
      )
      compress_gz(str_replace(zf[t], ".gz", ""))
    }
    
    rm(fitness_4, reflections_4, eigenvalues_4, rca_6)
  }
  
  # proximity 4 digits ----
  
  if (!file.exists(q[t]) | !file.exists(w[t])) {
    proximity_4 <- proximity(
      revealed_comparative_advantage = rca_4,
      country = "country_iso",
      product = "product_code",
      value = "export_rca",
      diversity = fitness_4$diversity,
      ubiquity = fitness_4$ubiquity,
      tbl_output = TRUE
    )
  }
  
  if (!file.exists(q[t])) {
    fwrite(proximity_4$proximity_countries, str_replace(q[t], ".gz", ""))
    compress_gz(str_replace(q[t], ".gz", ""))
  }
  
  if (!file.exists(w[t])) {
    fwrite(proximity_4$proximity_products, str_replace(w[t], ".gz", ""))
    compress_gz(str_replace(w[t], ".gz", ""))
  }
}
