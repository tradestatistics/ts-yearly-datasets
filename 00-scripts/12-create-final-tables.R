# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

summarise_trade <- function(d) {
  d %>% 
    summarise(
      export_value_usd = sum(export_value_usd, na.rm = T),
      import_value_usd = sum(import_value_usd, na.rm = T),
      
      export_value_usd_t2 = sum(export_value_usd_t2, na.rm = T),
      import_value_usd_t2 = sum(import_value_usd_t2, na.rm = T),
      
      export_value_usd_t3 = sum(export_value_usd_t3, na.rm = T),
      import_value_usd_t3 = sum(import_value_usd_t3, na.rm = T)
    )
}

compute_changes <- function(d) {
  d %>% 
    mutate(
      export_value_usd = ifelse(export_value_usd == 0, NA, export_value_usd),
      import_value_usd = ifelse(import_value_usd == 0, NA, import_value_usd),
      
      export_value_usd_t2 = ifelse(export_value_usd_t2 == 0, NA, export_value_usd_t2),
      import_value_usd_t2 = ifelse(import_value_usd_t2 == 0, NA, import_value_usd_t2),
      
      export_value_usd_t3 = ifelse(export_value_usd_t3 == 0, NA, export_value_usd_t3),
      import_value_usd_t3 = ifelse(import_value_usd_t3 == 0, NA, import_value_usd_t3)
    ) %>% 
    mutate(
      export_value_usd_change_1_year = export_value_usd - export_value_usd_t2,
      export_value_usd_change_5_years = export_value_usd - export_value_usd_t3,
      
      export_value_usd_percentage_change_1_year = export_value_usd_change_1_year / export_value_usd_t2,
      export_value_usd_percentage_change_5_years = export_value_usd_change_5_years / export_value_usd_t3,
      
      import_value_usd_change_1_year = import_value_usd - import_value_usd_t2,
      import_value_usd_change_5_years = import_value_usd - import_value_usd_t3,
      
      import_value_usd_percentage_change_1_year = import_value_usd_change_1_year / import_value_usd_t2,
      import_value_usd_percentage_change_5_years = import_value_usd_change_5_years / import_value_usd_t3
    )
}

compute_tables <- function(t) {
  if (file.exists(yrpc_gz[[t]])) {
    messageline()
    message(paste("yrpc table for the year", years_full[t], "exists. Skipping."))
  } else {
    messageline()
    message(paste("Creating yrpc table for the year", years_full[t]))
    
    # pci/eci data ------------------------------------------------------------
    
    eci <- fread2("05-metrics/hs-rev2007-eci/eci-joined-ranking.csv.gz")
    pci <- fread2("05-metrics/hs-rev2007-pci/pci-joined-ranking.csv.gz", character = c("commodity_code"))
    
    pci_4 <- pci %>% filter(commodity_code_length == 4)
    pci_6 <- pci %>% filter(commodity_code_length == 6)
    
    # yrpc ------------------------------------------------------------------
    
    exports_t1 <- fread2(unified_gz[t], character = "commodity_code") %>% 
      rename(export_value_usd = trade_value_usd) %>% 
      select(-commodity_code_length)
    
    imports_t1 <- exports_t1 %>% select(-year)
    names(imports_t1) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd")
    
    yrpc_t1 <- full_join(exports_t1, imports_t1, by = c("reporter_iso", "partner_iso", "commodity_code"))
    rm(exports_t1, imports_t1)
    
    if (t %in% match(years_missing_t_minus_1, years_full)) {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t2 = NA,
          import_value_usd_t2 = NA
        )
    } else {
      exports_t2 <- fread2(unified_gz[t - 1], character = "commodity_code") %>% 
        rename(export_value_usd_t2 = trade_value_usd) %>% 
        select(-c(year, commodity_code_length))

      imports_t2 <- exports_t2
      names(imports_t2) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t2")
      
      yrpc_t2 <- full_join(exports_t2, imports_t2, by = c("reporter_iso", "partner_iso", "commodity_code"))
      rm(exports_t2, imports_t2)
      
      yrpc_t1 <- yrpc_t1 %>% 
        left_join(yrpc_t2, by = c("reporter_iso", "partner_iso", "commodity_code"))
      
      rm(yrpc_t2)
    }
    
    if (t %in% match(years_missing_t_minus_5, years_full) | t %in% match(years_missing_t_minus_1, years_full)) {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t3 = NA,
          import_value_usd_t3 = NA
        )
    } else {
      exports_t3 <- fread2(unified_gz[t - 5], character = "commodity_code") %>% 
        rename(export_value_usd_t3 = trade_value_usd) %>% 
        select(-c(year, commodity_code_length))
      
      imports_t3 <- exports_t3
      names(imports_t3) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t3")
      
      yrpc_t3 <- full_join(exports_t3, imports_t3, by = c("reporter_iso", "partner_iso", "commodity_code"))
      rm(exports_t3, imports_t3)
      
      yrpc_t1 <- yrpc_t1 %>% 
        left_join(yrpc_t3, by = c("reporter_iso", "partner_iso", "commodity_code"))
      
      rm(yrpc_t3)
    }
    
    yrpc_t1 <- yrpc_t1 %>%
      mutate(commodity_code_length = str_length(commodity_code))
    
    yrpc <- yrpc_t1 %>% 
      compute_changes() %>% 
      select(year, matches("iso"), matches("commodity"), export_value_usd, import_value_usd, matches("change"))
    
    fwrite(yrpc, yrpc_csv[[t]])
    compress_gz(yrpc_csv[[t]])
    rm(yrpc)
    
    # yrp ------------------------------------------------------------------
    
    yrp <- yrpc_t1 %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrp, yrp_csv[[t]])
    compress_gz(yrp_csv[[t]])
    rm(yrp)
    
    # yrc ------------------------------------------------------------------
    
    rca_exp <- fread2(rca_exports_gz[[t]], character = "commodity_code") %>%
      select(-year)
    
    rca_exp_4 <- rca_exp %>% 
      filter(str_length(commodity_code) == 4) %>% 
      rename(export_rca_4_digits_commodity_code = export_rca)
    
    rca_exp_6 <- rca_exp %>% 
      filter(str_length(commodity_code) == 6) %>% 
      rename(export_rca_6_digits_commodity_code = export_rca)
    
    rm(rca_exp)
    
    rca_imp <- fread2(rca_imports_gz[[t]], character = "commodity_code") %>%
      select(-year)
    
    rca_imp_4 <- rca_imp %>% 
      filter(str_length(commodity_code) == 4) %>% 
      rename(import_rca_4_digits_commodity_code = import_rca)
    
    rca_imp_6 <- rca_imp %>% 
      filter(str_length(commodity_code) == 6) %>% 
      rename(import_rca_6_digits_commodity_code = import_rca)
    
    rm(rca_imp)
    
    yrc <- yrpc_t1 %>%
      group_by(year, reporter_iso, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>%
      left_join(rca_exp_4, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      left_join(rca_exp_6, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      left_join(rca_imp_4, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      left_join(rca_imp_6, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      compute_changes() %>% 
      select(year, reporter_iso, commodity_code, everything()) %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrc, yrc_csv[[t]])
    compress_gz(yrc_csv[[t]])
    rm(yrc, rca_exp, rca_imp)
    
    # ypc ------------------------------------------------------------------
    
    ypc <- yrpc_t1 %>%
      group_by(year, partner_iso, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>%
      compute_changes() %>%
      select(year, partner_iso, commodity_code, everything()) %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(ypc, ypc_csv[[t]])
    compress_gz(ypc_csv[[t]])
    rm(ypc)
    
    # yr -------------------------------------------------------------------
    
    max_exp <- yrpc_t1 %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_export_commodity_code = commodity_code,
        top_export_trade_value_usd = export_value_usd
      )
    
    max_imp <- yrpc_t1 %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_import_commodity_code = commodity_code,
        top_import_trade_value_usd = import_value_usd
      )
    
    yr <- yrpc_t1 %>%
      group_by(year, reporter_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      left_join(max_exp, by = "reporter_iso") %>%
      left_join(max_imp, by = "reporter_iso") %>%
      compute_changes() %>% 
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yr, yr_csv[[t]])
    compress_gz(yr_csv[[t]])
    rm(yr, max_exp, max_imp)
    
    # yp -------------------------------------------------------------------
    
    yp <- yrpc_t1 %>%
      group_by(year, partner_iso) %>%
      summarise_trade() %>%
      ungroup() %>%
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yp, yp_csv[[t]])
    compress_gz(yp_csv[[t]])
    rm(yp)
    
    # yc -------------------------------------------------------------------
    
    pci_t1 <- pci %>%
      filter(year == years_full[[t]]) %>%
      select(-year)
    
    pci_4_t1 <- pci_t1 %>% 
      filter(commodity_code_length == 4) %>% 
      rename(
        pci_4_digits_commodity_code = pci,
        pci_rank_4_digits_commodity_code = pci_rank
      ) %>% 
      select(-commodity_code_length)

    pci_6_t1 <- pci_t1 %>% 
      filter(commodity_code_length == 6) %>% 
      rename(
        pci_6_digits_commodity_code = pci,
        pci_rank_6_digits_commodity_code = pci_rank
      ) %>% 
      select(-commodity_code_length)
    
    if (t %in% match(years_missing_t_minus_1, years_full)) {
      pci_t2 <- pci_t1 %>%
        mutate(
          pci_rank = NA,
          pci = NA
        )
    } else {
      pci_t2 <- pci %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year)
    }
    
    pci_4_t2 <- pci_t2 %>% 
      filter(commodity_code_length == 4) %>% 
      rename(
        pci_4_digits_commodity_code_t2 = pci,
        pci_rank_4_digits_commodity_code_t2 = pci_rank
      ) %>% 
      select(-commodity_code_length)
    
    pci_6_t2 <- pci_t2 %>% 
      filter(commodity_code_length == 6) %>% 
      rename(
        pci_6_digits_commodity_code_t2 = pci,
        pci_rank_6_digits_commodity_code_t2 = pci_rank
      ) %>% 
      select(-commodity_code_length)
    
    rm(pci_t1)
    try(rm(pci_t2))
    
    max_exp_2 <- yrpc_t1 %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(commodity_code) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_exporter_iso = reporter_iso,
        top_exporter_trade_value_usd = export_value_usd
      )

    max_imp_2 <- yrpc_t1 %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(commodity_code) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_importer_iso = reporter_iso,
        top_importer_trade_value_usd = import_value_usd
      )
    
    yc <- yrpc_t1 %>%
      group_by(year, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>% 
      left_join(pci_4_t1, by = "commodity_code") %>%
      left_join(pci_6_t1, by = "commodity_code") %>%
      left_join(pci_4_t2, by = "commodity_code") %>%
      left_join(pci_6_t2, by = "commodity_code") %>%
      mutate(
        pci_rank_4_digits_commodity_code_delta = pci_rank_4_digits_commodity_code - pci_rank_4_digits_commodity_code_t2,
        pci_rank_6_digits_commodity_code_delta = pci_rank_6_digits_commodity_code - pci_rank_6_digits_commodity_code_t2
      ) %>%
      left_join(max_exp_2, by = "commodity_code") %>%
      left_join(max_imp_2, by = "commodity_code") %>%
      compute_changes() %>%
      select(year, commodity_code, export_value_usd, import_value_usd, pci_4_digits_commodity_code, pci_6_digits_commodity_code, everything()) %>% 
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yc, yc_csv[[t]])
    compress_gz(yc_csv[[t]])
  }
}
