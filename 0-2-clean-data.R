# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

clean <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # scripts -----------------------------------------------------------------

  ask_number_of_cores <<- 1
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")

  # functions ---------------------------------------------------------------

  compute_tidy_data <- function(t) {
    if (!file.exists(clean_rds[t])) {
      messageline()
      message(paste("Cleaning", years[t], "data..."))
      
      # CIF-FOB rate ------------------------------------------------------------
      
      # See Anderson & van Wincoop, 2004, Hummels, 2006 and Gaulier & Zignago, 2010 about 8% rate consistency
      cif_fob_rate <- 1.08
      
      # ISO-3 codes -------------------------------------------------------------
      
      load("../comtrade-codes/01-2-tidy-country-data/country-codes.RData")
      
      country_codes <- country_codes %>%
        select(iso3_digit_alpha) %>%
        mutate(iso3_digit_alpha = str_to_lower(iso3_digit_alpha)) %>%
        filter(!iso3_digit_alpha %in% c("wld", "null")) %>%
        as_vector()
      
      # clean data --------------------------------------------------------------
      
      clean_data <- readRDS(raw_rds[t]) %>% 
        select("Year", "Aggregate Level", "Trade Flow", "Reporter ISO", "Partner ISO", "Commodity Code", "Trade Value (US$)") %>%
        clean_names() %>% 
        rename(
          product_code = commodity_code,
          trade_value_usd = trade_value_us
        ) %>%
        filter(aggregate_level %in% J) %>%
        filter(trade_flow %in% c("Export", "Import")) %>%
        filter(
          !is.na(product_code),
          product_code != "",
          product_code != " "
        ) %>%
        mutate(
          reporter_iso = str_to_lower(reporter_iso),
          partner_iso = str_to_lower(partner_iso),
          
          reporter_iso = ifelse(reporter_iso == "rou", "rom", reporter_iso),
          partner_iso = ifelse(partner_iso == "rou", "rom", partner_iso)
        ) %>%
        filter(
          reporter_iso %in% country_codes,
          partner_iso %in% country_codes
        ) %>% 
        as_tibble()
      
      # exports data ------------------------------------------------------------
      
      exports <- clean_data %>%
        filter(trade_flow == "Export") %>%
        select(reporter_iso, partner_iso, product_code, trade_value_usd) %>%
        mutate(trade_value_usd = ceiling(trade_value_usd))
      
      exports_mirrored <- clean_data %>%
        filter(trade_flow == "Import") %>%
        select(reporter_iso, partner_iso, product_code, trade_value_usd) %>%
        mutate(trade_value_usd = ceiling(trade_value_usd / cif_fob_rate))
      
      colnames(exports_mirrored) <- c("partner_iso", "reporter_iso", "product_code", "trade_value_usd")
      
      rm(clean_data)
      
      exports_conciliated <- exports %>%
        full_join(exports_mirrored, by = c("reporter_iso", "partner_iso", "product_code")) %>%
        rowwise() %>%
        mutate(trade_value_usd = max(trade_value_usd.x, trade_value_usd.y, na.rm = T)) %>%
        ungroup() %>%
        mutate(product_code_parent = str_sub(product_code, 1, 4)) %>%
        group_by(reporter_iso, partner_iso, product_code_parent) %>%
        mutate(parent_count = n()) %>%
        ungroup() %>%
        select(reporter_iso, partner_iso, product_code, product_code_parent, parent_count, trade_value_usd)
      
      rm(exports, exports_mirrored)
      
      exports_conciliated_unrepeated_parent <- exports_conciliated %>%
        filter(parent_count == 1)
      
      exports_conciliated_repeated_parent <- exports_conciliated %>%
        filter(
          parent_count > 1,
          str_length(product_code) %in% c(5, 6)
        )
      
      exports_conciliated_repeated_parent_summary <- exports_conciliated_repeated_parent %>%
        group_by(reporter_iso, partner_iso, product_code_parent) %>%
        summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
        ungroup() %>%
        rename(product_code = product_code_parent)
      
      exports_conciliated <- exports_conciliated_unrepeated_parent %>%
        bind_rows(exports_conciliated_repeated_parent) %>%
        bind_rows(exports_conciliated_repeated_parent_summary) %>%
        arrange(reporter_iso, partner_iso, product_code) %>%
        mutate(
          year = years[t],
          product_code_length = str_length(product_code)
        ) %>%
        select(year, reporter_iso, partner_iso, product_code, product_code_length, trade_value_usd) %>%
        filter(trade_value_usd > 0)
      
      rm(exports_conciliated_unrepeated_parent, exports_conciliated_repeated_parent, exports_conciliated_repeated_parent_summary)
      
      saveRDS(exports_conciliated, file = clean_rds[t], compress = "xz")
    } else {
      messageline()
      message(paste("Skipping year", years[t], "Files exist."))
    }
  }
  
  # create tidy datasets ----------------------------------------------------

  messageline()
  message("Rearranging files. Please wait...")

  if (operating_system != "Windows") {
    mclapply(seq_along(raw_zip), compute_tidy_data, mc.cores = n_cores)
  } else {
    lapply(seq_along(raw_zip), compute_tidy_data)
  }
}

clean()
