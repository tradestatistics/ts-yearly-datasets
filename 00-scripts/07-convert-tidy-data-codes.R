# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

convert_codes <- function(t, x, y) {
  # product codes -----------------------------------------------------------
  
  load("../ts-comtrade-codes/02-2-tidy-product-data/product-conversion.RData")
  #load("../ts-comtrade-codes/02-2-tidy-product-data/product-correlation.RData")
  
  # convert data ------------------------------------------------------------
  
  convert_to <- c2[4]
  
  product_conversion <- product_conversion %>% 
    select(!!sym(c2[dataset]), !!sym(convert_to)) %>% 
    arrange(!!sym(c2[dataset]), !!sym(convert_to)) %>% 
    filter(
      !(!!sym(c2[dataset]) %in% c("NULL")),
      !(!!sym(convert_to) %in% c("NULL")),
      str_length(!!sym(c2[dataset])) %in% c(4,5),
      str_length(!!sym(convert_to)) %in% c(4,6)
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
    data <- fread2(x[t], char = c("commodity_code"), num = c("trade_value_usd")) %>% 
      mutate(commodity_code_parent = str_sub(commodity_code, 1, 4)) %>% 
      left_join(product_conversion %>% select(!!sym(c2[dataset]), !!sym(convert_to)), by = c("commodity_code" = c2[dataset])) %>% 
      left_join(product_conversion_missing_parent_codes, by = c("commodity_code_parent" = "original_code_parent")) %>% 
      mutate(
        !!sym(convert_to) := ifelse(is.na(!!sym(convert_to)), converted_code_parent, !!sym(convert_to)),
        !!sym(convert_to) := ifelse(is.na(!!sym(convert_to)), "9999", !!sym(convert_to)),
        converted_code_parent = str_sub(!!sym(convert_to), 1, 4)
      ) %>% 
      group_by(reporter_iso, partner_iso, converted_code_parent) %>% 
      mutate(parent_count = n()) %>% 
      ungroup()
    
    data_unrepeated_parent <- data %>% 
      filter(parent_count == 1) %>% 
      select(year, reporter_iso, partner_iso, !!sym(convert_to), converted_code_parent, trade_value_usd)
    
    data_unrepeated_parent_summary <- data_unrepeated_parent %>% 
      filter(str_length(!!sym(convert_to)) == 6) %>% 
      group_by(reporter_iso, partner_iso, converted_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      rename(!!sym(convert_to) := converted_code_parent)

    data_repeated_parent <- data %>% 
      filter(
        parent_count > 1,
        str_length(!!sym(convert_to)) == 6 | !!sym(convert_to) == "9999"
      ) %>% 
      select(year, reporter_iso, partner_iso, !!sym(convert_to), converted_code_parent, trade_value_usd)
    
    data_repeated_parent_summary <- data_repeated_parent %>% 
      group_by(reporter_iso, partner_iso, converted_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      rename(!!sym(convert_to) := converted_code_parent)
    
    data <- data_unrepeated_parent %>% 
      bind_rows(data_unrepeated_parent_summary) %>% 
      bind_rows(data_repeated_parent) %>% 
      bind_rows(data_repeated_parent_summary) %>% 
      select(year, reporter_iso, partner_iso, !!sym(convert_to), trade_value_usd) %>% 
      rename(commodity_code = !!sym(convert_to)) %>% 
      arrange(reporter_iso, partner_iso, commodity_code) %>% 
      mutate(
        year = years[t],
        commodity_code_length = str_length(commodity_code)
      ) %>% 
      select(year, reporter_iso, partner_iso, commodity_code, commodity_code_length, trade_value_usd) %>% 
      filter(trade_value_usd > 0)
    
    rm(data_unrepeated_parent, data_repeated_parent, data_repeated_parent_summary)
    
    fwrite(data, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
  }
}
