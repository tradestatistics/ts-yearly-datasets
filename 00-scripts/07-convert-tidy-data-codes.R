# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

convert_codes <- function(t, x, y) {
  # product codes -----------------------------------------------------------

  load("../comtrade-codes/02-2-tidy-product-data/product-conversion.RData")
  # load("../comtrade-codes/02-2-tidy-product-data/product-correlation.RData")

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
    ) %>% 
    select(original_code_parent, converted_code_parent) %>% 
    distinct(original_code_parent, .keep_all = T)

  if (!file.exists(y[t])) {
    data <- fread2(x[t], character = "product_code", numeric = "trade_value_usd") %>%
      left_join(product_conversion, by = c("product_code" = "original_code_parent")) %>%
      mutate(
        converted_code_parent = if_else(is.na(converted_code_parent), "9999", converted_code_parent)
      ) %>%
      select(-product_code) %>%
      rename(product_code = converted_code_parent) %>%
      group_by(reporter_iso, partner_iso, product_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      select(reporter_iso, partner_iso, product_code, trade_value_usd) %>% 
      filter(trade_value_usd > 0)

    fwrite(data, str_replace(y[t], ".gz", ""))
    compress_gz(str_replace(y[t], ".gz", ""))
  }
}
