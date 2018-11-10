# Open ts-yearly-data.Rproj before running this function

convert_codes <- function(t, x, y, z) {
  # product codes -----------------------------------------------------------
  
  load("../ts-comtrade-codes/02-2-tidy-product-data/product-correspondence.RData")
  
  # convert data ------------------------------------------------------------
  
  convert_to <- c2[[dataset2]]
  
  equivalent_codes_high_granularity <- product_correspondence %>% 
    select(!!sym(c2[[dataset]]), !!sym(convert_to)) %>% 
    arrange(!!sym(c2[[dataset]]), !!sym(convert_to)) %>% 
    filter(
      !(!!sym(c2[[dataset]]) %in% c("NULL")),
      !(!!sym(convert_to) %in% c("NULL"))
    ) %>% 
    distinct(!!sym(c2[[dataset]]), .keep_all = T)
  
  equivalent_codes_low_granularity <- equivalent_codes_high_granularity %>% 
    mutate_if(is.character, str_sub, 1, 4) %>% 
    distinct(!!sym(c2[[dataset]]), .keep_all = T)
  
  equivalent_codes <- equivalent_codes_low_granularity %>% 
    bind_rows(equivalent_codes_high_granularity) %>% 
    arrange(!!sym(c2[[dataset]]), !!sym(convert_to))
  
  rm(equivalent_codes_high_granularity, equivalent_codes_low_granularity)
  
  if (!file.exists(z[[t]])) {
    data <- fread2(x[[t]], char = c("commodity_code"), num = c("trade_value_usd")) %>% 
      mutate(commodity_code_parent = str_sub(commodity_code, 1, 4)) %>% 
      left_join(equivalent_codes, by = c("commodity_code" = c2[[dataset]])) %>%
      left_join(equivalent_codes, by = c("commodity_code_parent" = c2[[dataset]]))
    
    na_equivalents <- data %>% 
      select(!!!syms(paste0(convert_to, c(".x", ".y")))) %>% 
      filter(
        is.na(!!sym(paste0(convert_to, ".x"))) & 
          !is.na(!!sym(paste0(convert_to, ".y")))
      ) %>% 
      select(!!sym(paste0(convert_to, ".y"))) %>% 
      distinct() %>% 
      as_vector()
    
    data_na_equivalents <- data %>% 
      filter(!!sym(paste0(convert_to, ".y")) %in% na_equivalents) %>% 
      group_by(year, reporter_iso, partner_iso, !!sym(paste0(convert_to, ".y"))) %>% 
      summarise(trade_value_usd = sum(trade_value_usd)) %>% 
      ungroup() %>% 
      rename(commodity_code = !!sym(paste0(convert_to, ".y"))) %>% 
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)
    
    data_non_na_equivalents <- data %>% 
      filter(!(!!sym(paste0(convert_to, ".y")) %in% na_equivalents)) %>% 
      select(-commodity_code) %>% 
      rename(commodity_code = !!sym(paste0(convert_to, ".x"))) %>% 
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)
    
    data <- data_na_equivalents %>% 
      bind_rows(data_non_na_equivalents) %>% 
      mutate(commodity_code = ifelse(is.na(commodity_code), "9999", commodity_code)) %>% 
      group_by(reporter_iso, partner_iso, commodity_code) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      mutate(commodity_code_parent = str_sub(commodity_code, 1, 4)) %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      mutate(parent_count = n()) %>% 
      ungroup() %>% 
      select(reporter_iso, partner_iso, commodity_code, commodity_code_parent, parent_count, trade_value_usd)
    
    rm(data_na_equivalents, data_non_na_equivalents)
    
    data_unrepeated_parent <- data %>% 
      filter(parent_count == 1)
    
    data_repeated_parent <- data %>% 
      filter(
        parent_count > 1,
        str_length(commodity_code) %in% c(5,6)
      )
    
    data_repeated_parent_summary <- data_repeated_parent %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      rename(commodity_code = commodity_code_parent)
    
    data <- data_unrepeated_parent %>% 
      bind_rows(data_repeated_parent) %>% 
      bind_rows(data_repeated_parent_summary) %>% 
      arrange(reporter_iso, partner_iso, commodity_code) %>% 
      mutate(
        year = years[[t]],
        commodity_code_length = str_length(commodity_code)
      ) %>% 
      select(year, reporter_iso, partner_iso, commodity_code, commodity_code_length, trade_value_usd) %>% 
      filter(trade_value_usd > 0)
    
    rm(data_unrepeated_parent, data_repeated_parent, data_repeated_parent_summary)
    
    fwrite(data, y[[t]])
    compress_gz(y[[t]])
  }
}
