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
      export_value_usd_change_1year = export_value_usd - export_value_usd_t2,
      export_value_usd_change_5years = export_value_usd - export_value_usd_t3,
      
      export_value_usd_percentage_change_1year = export_value_usd_change_1year / export_value_usd_t2,
      export_value_usd_percentage_change_5years = export_value_usd_change_5years / export_value_usd_t3,
      
      import_value_usd_change_1year = import_value_usd - import_value_usd_t2,
      import_value_usd_change_5years = import_value_usd - import_value_usd_t3,
      
      import_value_usd_percentage_change_1year = import_value_usd_change_1year / import_value_usd_t2,
      import_value_usd_percentage_change_5years = import_value_usd_change_5years / import_value_usd_t3
    )
}

compute_tables <- function(t) {
  if (file.exists(yrpc_gz[[t]])) {
    messageline()
    message(paste("yrpc table for the year", years[t], "exists. Skipping."))
  } else {
    messageline()
    message(paste("Creating yrpc table for the year", years[t]))
    
    # yrpc ------------------------------------------------------------------
    
    exports_t1 <- fread2(unified_gz[t], char = "commodity_code") %>% 
      rename(export_value_usd = trade_value_usd)
    
    exports_t1_4 <- exports_t1 %>% 
      filter(commodity_code_length == 4) %>% 
      select(-commodity_code_length)
    
    exports_t1_6 <- exports_t1 %>% 
      filter(commodity_code_length == 6) %>% 
      select(-commodity_code_length)
    
    rm(exports_t1)
    
    imports_t1_4 <- exports_t1_4 %>% select(-year)
    names(imports_t1_4) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd")
    
    imports_t1_6 <- exports_t1_6 %>% select(-year)
    names(imports_t1_6) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd")
    
    yrpc_t1_4 <- full_join(exports_t1_4, imports_t1_4, by = c("reporter_iso", "partner_iso", "commodity_code"))
    rm(exports_t1_4, imports_t1_4)
    
    yrpc_t1_6 <- full_join(exports_t1_6, imports_t1_6, by = c("reporter_iso", "partner_iso", "commodity_code"))
    rm(exports_t1_6, imports_t1_6)
    
    if (t %in% match(years_missing_t_minus_1, years)) {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t2 = NA,
          import_value_usd_t2 = NA
        )
    } else {
      exports_t2 <- fread2(unified_gz[t - 1], char = "commodity_code") %>% 
        rename(export_value_usd_t2 = trade_value_usd) %>% 
        select(-year)

      exports_t2_4 <- exports_t2 %>% 
        filter(commodity_code_length == 4) %>% 
        select(-commodity_code_length)
      
      exports_t2_6 <- exports_t2 %>% 
        filter(commodity_code_length == 6) %>% 
        select(-commodity_code_length)
      
      rm(exports_t2)
      
      imports_t2_4 <- exports_t2_4
      names(imports_t2_4) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t2")
      
      imports_t2_6 <- exports_t2_6
      names(imports_t2_6) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t2")
      
      yrpc_t2_4 <- full_join(exports_t2_4, imports_t2_4, by = c("reporter_iso", "partner_iso", "commodity_code"))
      rm(exports_t2_4, imports_t2_4)
      
      yrpc_t2_6 <- full_join(exports_t2_6, imports_t2_6, by = c("reporter_iso", "partner_iso", "commodity_code"))
      rm(exports_t2_6, imports_t2_6)
      
      yrpc_t1_4 <- yrpc_t1_4 %>% 
        left_join(yrpc_t2_4)
      
      rm(yrpc_t2_4)
      
      yrpc_t1_6 <- yrpc_t1_6 %>% 
        left_join(yrpc_t2_6)
      
      rm(yrpc_t2_6)
    }
    
    if (t %in% match(years_missing_t_minus_5, years) | t %in% match(years_missing_t_minus_1, years)) {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t3 = NA,
          import_value_usd_t3 = NA
        )
    } else {
      exports_t3 <- fread2(unified_gz[t - 5], char = "commodity_code") %>% 
        rename(export_value_usd_t3 = trade_value_usd)
      
      exports_t3_4 <- exports_t3 %>% 
        filter(commodity_code_length == 4) %>% 
        select(-commodity_code_length)
      
      exports_t3_6 <- exports_t3 %>% 
        filter(commodity_code_length == 6) %>% 
        select(-commodity_code_length)
      
      rm(exports_t3)
      
      imports_t3_4 <- exports_t3_4 %>% select(-year)
      names(imports_t3_4) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t3")
      
      imports_t3_6 <- exports_t3_6 %>% select(-year)
      names(imports_t3_6) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t3")
      
      yrpc_t3_4 <- full_join(exports_t3_4, imports_t3_4, by = c("reporter_iso", "partner_iso", "commodity_code"))
      rm(exports_t3_4, imports_t3_4)
      
      yrpc_t3_6 <- full_join(exports_t3_6, imports_t3_6, by = c("reporter_iso", "partner_iso", "commodity_code"))
      rm(exports_t3_6, imports_t3_6)
      
      yrpc_t3_4 <- bind_rows(yrpc_t3_4, yrpc_t3_6)
      yrpc_t3 <- yrpc_t3_4
      rm(yrpc_t3_4, yrpc_t3_6)
    }
    
    yrpc_t1 <- yrpc_t1 %>%
      mutate(
        export_value_usd = ifelse(export_value_usd == 0, NA, export_value_usd),
        import_value_usd = ifelse(import_value_usd == 0, NA, import_value_usd)
      )
    
    if (class(yrpc_t2$export_value_usd_t2) != "logical") {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t2 = ifelse(export_value_usd_t2 == 0, NA, export_value_usd_t2),
          import_value_usd_t2 = ifelse(import_value_usd_t2 == 0, NA, import_value_usd_t2)
        )
    }
    
    if (class(yrpc_t3$export_value_usd_t3) != "logical") {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t3 = ifelse(export_value_usd_t3 == 0, NA, export_value_usd_t3),
          import_value_usd_t3 = ifelse(import_value_usd_t3 == 0, NA, import_value_usd_t3)
        )
    }
    
    yrpc_t1 <- yrpc_t1 %>%
      mutate(commodity_code_length = str_length(commodity_code)) %>% 
      compute_changes()

    if (class(yrpc$export_value_usd_t2) != "numeric") {
      yrpc <- yrpc %>%
        mutate(
          export_value_usd_t2 = as.double(export_value_usd_t2),
          export_value_usd_t3 = as.double(export_value_usd_t3),
          import_value_usd_t2 = as.double(import_value_usd_t2),
          import_value_usd_t3 = as.double(import_value_usd_t3)
        )
    }
    
    rm(yrpc_t1, yrpc_t2, yrpc_t3)
    
    fwrite(
      yrpc %>%
        select(-c(ends_with("_t2"), ends_with("_t3"))),
      yrpc_csv[[t]]
    )
    
    compress_gz(yrpc_csv[[t]])
    
    # yrp ------------------------------------------------------------------
    
    yrp <- yrpc %>%
      select(year, reporter_iso, partner_iso, matches("export"), matches("import")) %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrp, yrp_csv[[t]])
    compress_gz(yrp_csv[[t]])
    rm(yrp)
    
    # yrc ------------------------------------------------------------------
    
    rca_exp <- fread4(rca_exports_gz[[t]]) %>%
      select(-year)
    
    rca_imp <- fread4(rca_imports_gz[[t]]) %>%
      select(-year)
    
    yrc <- yrpc %>%
      select(year, reporter_iso, commodity_code, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, reporter_iso, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>%
      left_join(rca_exp, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      left_join(rca_imp, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      compute_changes() %>% 
      select(year, reporter_iso, commodity_code, everything()) %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrc, yrc_csv[[t]])
    compress_gz(yrc_csv[[t]])
    rm(yrc, rca_exp, rca_imp)
    
    # ypc ------------------------------------------------------------------
    
    ypc <- yrpc %>%
      select(year, partner_iso, commodity_code, matches("export_value_usd"), matches("import_value_usd")) %>%
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
    
    max_exp <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_export_commodity_code = commodity_code,
        top_export_value_usd = export_value_usd
      )
    
    max_imp <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_import_commodity_code = commodity_code,
        top_import_value_usd = import_value_usd
      )
    
    yr <- yrpc %>%
      select(year, reporter_iso, matches("export_value_usd"), matches("import_value_usd")) %>%
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
    
    yp <- yrpc %>%
      select(year, partner_iso, matches("export_value_usd"), matches("import_value_usd")) %>%
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
      filter(year == years[[t]]) %>%
      select(-year)
    
    if (t %in% match(years_missing_t_minus_1, years)) {
      pci_t2 <- pci_t1 %>%
        mutate(
          pci_rank = NA,
          pci = NA
        )
    } else {
      pci_t2 <- pci %>%
        filter(year == years[[t - 1]]) %>%
        select(-year)
    }
    
    max_exp_2 <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(commodity_code) %>%
      slice(which.max(export_value_usd)) %>%
      rename(top_exporter_iso = reporter_iso) %>%
      select(-export_value_usd)
    
    max_imp_2 <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(commodity_code) %>%
      slice(which.max(import_value_usd)) %>%
      rename(top_importer_iso = reporter_iso) %>%
      select(-import_value_usd)
    
    yc <- yrpc %>%
      select(year, commodity_code, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>% 
      left_join(pci_t1, by = "commodity_code") %>%
      left_join(pci_t2, by = "commodity_code") %>%
      mutate(pci_rank_delta = pci_rank.x - pci_rank.y) %>%
      select(-c(pci_rank.y, pci.y)) %>%
      rename(
        pci = pci.x,
        pci_rank = pci_rank.x
      ) %>%
      left_join(max_exp_2, by = "commodity_code") %>%
      left_join(max_imp_2, by = "commodity_code") %>%
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3"))) %>% 
      select(year, commodity_code, everything())
    
    fwrite(yc, yc_csv[[t]])
    compress_gz(yc_csv[[t]])
    rm(yrpc, yc, pci_t1, pci_t2, max_exp_2, max_imp_2)
  }
}
