rca <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2021, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # helpers -----------------------------------------------------------------

  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")

  # functions ---------------------------------------------------------------

  compute_rca <- function(x, y, t, group_field) {
    if (file.exists(y[t])) {
      messageline()
      message(paste0("Skipping year ", years_full[t], ". The file already exist."))
    } else {
      messageline()
      message(paste0(
        "Creating smooth RCA file for the year ", years_full[t], ". Be patient..."
      ))
      
      trade_t1 <- readRDS(x[t]) %>%
        group_by(!!sym(group_field), product_code) %>%
        summarise(trade_value_usd_t1 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
      
      if (years_full[t] <= years_missing_t_minus_1) {
        trade_t1 <- trade_t1 %>%
          mutate(trade_value_usd_t2 = NA)
      } else {
        trade_t2 <- readRDS(x[t - 1]) %>%
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
        trade_t3 <- readRDS(x[t - 2]) %>%
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
        select(-c(trade_value_usd_t1, trade_value_usd_t2, trade_value_usd_t3))
      
      trade_t1 <- trade_t1 %>% 
        balassa_index(
          country = group_field,
          product = "product_code",
          value = "xcp",
          discrete = F
        )
      
      trade_t1 <- trade_t1 %>% 
        as.matrix() %>% 
        as.data.frame() %>% 
        tibble::rownames_to_column("country") %>% 
        gather("product", "value", -country) %>% 
        as_tibble()
        
      trade_t1 <- trade_t1 %>% 
        rename(!!sym(group_field) := country, product_code = product) %>%
        mutate(year = years_full[t]) %>%
        select(year, !!sym(group_field), product_code, value)
      
      if (group_field == "reporter_iso") {
        names(trade_t1) <- c("year", "country_iso", "product_code", "export_rca")
      } else {
        names(trade_t1) <- c("year", "country_iso", "product_code", "import_rca")
      }
      
      saveRDS(trade_t1, file = y[t], compress = "xz")
    }
  }
  
  # RCA ----

  lapply(seq_along(years_full), compute_rca,
    x = unified_rds, y = rca_exports_rds, group_field = "reporter_iso"
  )

  lapply(seq_along(years_full), compute_rca,
    x = unified_rds, y = rca_imports_rds, group_field = "partner_iso"
  )
}

rca()
