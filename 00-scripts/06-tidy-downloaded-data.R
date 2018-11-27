# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

compute_tidy_data <- function(t) {
  if (!file.exists(clean_gz[t])) {
    messageline()
    message(paste("Cleaning", years[t], "data..."))
    
    # CIF-FOB rate ------------------------------------------------------------
    
    # See Anderson & van Wincoop, 2004, Hummels, 2006 and Gaulier & Zignago, 2010 about 8% rate consistency
    cif_fob_rate <- 1.08
    
    # ISO-3 codes -------------------------------------------------------------
    
    load("../ts-comtrade-codes/01-2-tidy-country-data/country-codes.RData")
    
    country_codes <- country_codes %>% 
      select(iso3_digit_alpha) %>% 
      mutate(iso3_digit_alpha = str_to_lower(iso3_digit_alpha)) %>% 
      filter(!iso3_digit_alpha %in% c("wld","null")) %>% 
      as_vector()
    
    # clean data --------------------------------------------------------------
    
    clean_data <- fread2(str_replace(raw_zip[t], "zip", "csv"),
                         select = c("Year", "Aggregate Level", "Trade Flow", "Reporter ISO", "Partner ISO", "Commodity Code", "Trade Value (US$)"),
                         character = "Commodity Code",
                         numeric = "Trade Value (US$)"
    ) %>%
      
      rename(trade_value_usd = trade_value_us) %>%
      
      filter(aggregate_level %in% J) %>%
      filter(trade_flow %in% c("Export","Import")) %>%
      
      filter(
        !is.na(commodity_code),
        commodity_code != "",
        commodity_code != " "
      ) %>%
      
      mutate(
        reporter_iso = str_to_lower(reporter_iso),
        partner_iso = str_to_lower(partner_iso)
      ) %>%
      
      filter(
        reporter_iso %in% country_codes,
        partner_iso %in% country_codes
      )
    
    # exports data ------------------------------------------------------------
    
    exports <- clean_data %>%
      filter(trade_flow == "Export") %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd) %>% 
      mutate(trade_value_usd = ceiling(trade_value_usd))
    
    exports_mirrored <- clean_data %>%
      filter(trade_flow == "Import") %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd) %>% 
      mutate(trade_value_usd = ceiling(trade_value_usd / cif_fob_rate))
    
    colnames(exports_mirrored) <- c("partner_iso", "reporter_iso", "commodity_code", "trade_value_usd")
    
    rm(clean_data)
    
    exports_model <- exports %>% 
      full_join(exports_mirrored, by = c("reporter_iso", "partner_iso", "commodity_code")) %>% 
      rowwise() %>% 
      mutate(trade_value_usd = max(trade_value_usd.x, trade_value_usd.y, na.rm = T)) %>% 
      ungroup() %>% 
      mutate(commodity_code_parent = str_sub(commodity_code, 1, 4)) %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      mutate(parent_count = n()) %>% 
      ungroup() %>% 
      select(reporter_iso, partner_iso, commodity_code, commodity_code_parent, parent_count, trade_value_usd)
    
    rm(exports, exports_mirrored)
    
    exports_model_unrepeated_parent <- exports_model %>% 
      filter(parent_count == 1)
    
    exports_model_repeated_parent <- exports_model %>% 
      filter(
        parent_count > 1,
        str_length(commodity_code) %in% c(5,6)
      )
    
    exports_model_repeated_parent_summary <- exports_model_repeated_parent %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      rename(commodity_code = commodity_code_parent)
    
    exports_model <- exports_model_unrepeated_parent %>% 
      bind_rows(exports_model_repeated_parent) %>% 
      bind_rows(exports_model_repeated_parent_summary) %>% 
      arrange(reporter_iso, partner_iso, commodity_code) %>% 
      mutate(
        year = years[t],
        commodity_code_length = str_length(commodity_code)
      ) %>% 
      select(year, reporter_iso, partner_iso, commodity_code, commodity_code_length, trade_value_usd) %>% 
      filter(trade_value_usd > 0)
    
    rm(exports_model_unrepeated_parent, exports_model_repeated_parent, exports_model_repeated_parent_summary)
    
    fwrite(exports_model, str_replace(clean_gz[t], ".gz", ""))
    compress_gz(str_replace(clean_gz[t], ".gz", ""))
  } else {
    messageline()
    message(paste("Skipping year", years[t], "Files exist."))
  }
}
