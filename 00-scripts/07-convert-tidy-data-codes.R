# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

convert_codes <- function(t, x, y) {
  ## product codes -----------------------------------------------------------
  
  load("../comtrade-codes/02-2-tidy-product-data/product-conversion.RData")

  # convert data ------------------------------------------------------------
  
  convert_to <- c2[4]
  
  product_conversion <- product_conversion %>%
    select(!!sym(c2[dataset]), !!sym(convert_to)) %>%
    arrange(!!sym(c2[dataset]), !!sym(convert_to)) %>%
    filter(
      !(!!sym(c2[dataset]) %in% c("NULL")),
      !(!!sym(convert_to) %in% c("NULL")),
      str_length(!!sym(c2[dataset])) %in% 4:6,
      str_length(!!sym(convert_to)) %in% c(4, 6)
    ) %>%
    distinct(!!sym(c2[dataset]), .keep_all = T) %>%
    mutate(
      original_code_parent = str_sub(!!sym(c2[dataset]), 1, 4),
      converted_code_parent = str_sub(!!sym(convert_to), 1, 4)
    )
  
  product_conversion_missing_parent_codes <- product_conversion %>%
    select(original_code_parent, converted_code_parent) %>%
    distinct(original_code_parent, .keep_all = T) %>%
    anti_join(product_conversion, by = c("original_code_parent" = c2[dataset]))
  
  if (!file.exists(y[t])) {
    data <- fread2(x[t], character = "product_code", numeric = "trade_value_usd") %>%
      select(-year) %>%
      mutate(product_code_parent = str_sub(product_code, 1, 4)) %>%
      left_join(product_conversion %>% select(!!sym(c2[dataset]), !!sym(convert_to)), by = c("product_code" = c2[dataset])) %>%
      left_join(product_conversion_missing_parent_codes, by = c("product_code_parent" = "original_code_parent")) %>%
      mutate(
        !!sym(convert_to) := if_else(is.na(!!sym(convert_to)), converted_code_parent, !!sym(convert_to)),
        !!sym(convert_to) := if_else(is.na(!!sym(convert_to)), "9999", !!sym(convert_to)),
        !!sym(convert_to) := if_else(str_sub(!!sym(convert_to), 1, 4) == "9999", "9999", !!sym(convert_to)),
        converted_code_parent = str_sub(!!sym(convert_to), 1, 4)
      ) %>%
      select(-c(product_code, product_code_length, product_code_parent)) %>%
      rename(
        product_code = !!sym(convert_to),
        product_code_parent = converted_code_parent
      ) %>%
      group_by(reporter_iso, partner_iso, product_code_parent) %>%
      mutate(parent_count = n()) %>%
      ungroup()
    
    data_unrepeated_parent <- data %>%
      filter(parent_count == 1)
    
    data_unrepeated_parent_4 <- data_unrepeated_parent %>%
      filter(str_length(product_code) == 4)
    
    data_unrepeated_parent_6 <- data_unrepeated_parent %>%
      filter(str_length(product_code) == 6)
    
    rm(data_unrepeated_parent)
    
    data_unrepeated_parent_6_summary <- data_unrepeated_parent_6 %>%
      group_by(reporter_iso, partner_iso, product_code_parent) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      ungroup() %>%
      rename(product_code = product_code_parent)
    
    data_unrepeated_parent_tidy <- data_unrepeated_parent_4 %>%
      bind_rows(data_unrepeated_parent_6_summary) %>%
      bind_rows(data_unrepeated_parent_6) %>%
      arrange(reporter_iso, partner_iso, product_code) %>%
      select(reporter_iso, partner_iso, product_code, trade_value_usd)
    
    rm(data_unrepeated_parent_4, data_unrepeated_parent_6, data_unrepeated_parent_6_summary)
    
    data_repeated_parent <- data %>%
      filter(
        parent_count > 1,
        str_length(product_code) %in% c(5, 6)
      ) %>%
      group_by(reporter_iso, partner_iso, product_code, product_code_parent) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      ungroup()
    
    data_repeated_parent_summary <- data_repeated_parent %>%
      group_by(reporter_iso, partner_iso, product_code_parent) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      ungroup() %>%
      rename(product_code = product_code_parent)
    
    data_repeated_parent_tidy <- data_repeated_parent %>%
      bind_rows(data_repeated_parent_summary) %>%
      arrange(reporter_iso, partner_iso, product_code) %>%
      select(reporter_iso, partner_iso, product_code, trade_value_usd)
    
    rm(data, data_repeated_parent, data_repeated_parent_summary)
    
    data <- data_unrepeated_parent_tidy %>%
      bind_rows(data_repeated_parent_tidy) %>%
      arrange(reporter_iso, partner_iso, product_code) %>%
      mutate(
        year = years[t],
        product_code_length = str_length(product_code)
      ) %>%
      select(year, reporter_iso, partner_iso, product_code, product_code_length, trade_value_usd) %>%
      filter(trade_value_usd > 0)
    
    rm(data_unrepeated_parent_tidy, data_repeated_parent_tidy)
    
    fwrite(data, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
  }
}
