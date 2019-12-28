# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

summarise_trade <- function(d) {
  d %>%
    summarise(
      export_value_usd = sum(export_value_usd, na.rm = T),
      import_value_usd = sum(import_value_usd, na.rm = T)
    )
}

compute_tables <- function(t) {
  # PCI/ECI data ------------------------------------------------------------

  eci_f <- fread2("05-metrics/hs-rev2007-eci/eci-fitness-joined-ranking.csv.gz")
  eci_r <- fread2("05-metrics/hs-rev2007-eci/eci-reflections-joined-ranking.csv.gz")
  eci_e <- fread2("05-metrics/hs-rev2007-eci/eci-eigenvalues-joined-ranking.csv.gz")

  pci_f <- fread2("05-metrics/hs-rev2007-pci/pci-fitness-joined-ranking.csv.gz", character = c("product"))
  pci_r <- fread2("05-metrics/hs-rev2007-pci/pci-reflections-joined-ranking.csv.gz", character = c("product"))
  pci_e <- fread2("05-metrics/hs-rev2007-pci/pci-eigenvalues-joined-ranking.csv.gz", character = c("product"))

  # YRPC ------------------------------------------------------------------

  if (!file.exists(yrpc_gz[t])) {
    message(paste("Creating YRPC table for the year", years_full[t]))

    exports <- fread2(unified_gz[t], character = "product_code") %>%
      rename(export_value_usd = trade_value_usd) %>% 
      select(-c(year, product_code_length))

    imports <- exports
    names(imports) <- c("partner_iso", "reporter_iso", "product_code", "import_value_usd")

    yrpc <- full_join(exports, imports, by = c("reporter_iso", "partner_iso", "product_code")) %>% 
      mutate(year = years_full[t]) %>%
      select(year, matches("iso"), matches("product"), export_value_usd, import_value_usd)
    
    yrpc <- yrpc %>%
      rowwise() %>% 
      mutate(trade_balance = sum(export_value_usd, import_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      filter(trade_balance > 0) %>% 
      select(-trade_balance)
    
    rm(exports, imports)

    fwrite(yrpc, yrpc_csv[[t]])
    compress_gz(yrpc_csv[[t]])
  } else {
    message(paste("Reading YRPC table for the year", years_full[t]))

    yrpc <- fread2(
      yrpc_gz[t],
      character = "product_code",
      numeric = c(
        "export_value_usd",
        "import_value_usd"
      )
    )
  }

  # YRP ------------------------------------------------------------------

  if (!file.exists(yrp_gz[t])) {
    yrp <- yrpc %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise_trade() %>%
      ungroup()
    
    yrp <- yrp %>% 
      rowwise() %>% 
      mutate(trade_balance = sum(export_value_usd, import_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      filter(trade_balance > 0) %>% 
      select(-trade_balance)
    
    fwrite(yrp, yrp_csv[[t]])
    compress_gz(yrp_csv[[t]])
    rm(yrp)
  }

  # YRC ------------------------------------------------------------------

  if (!file.exists(yrc_gz[t])) {
    rca_exp <- fread2(rca_exports_gz[[t]], character = "product_code") %>%
      select(-year)

    rca_imp <- fread2(rca_imports_gz[[t]], character = "product_code") %>%
      select(-year)
    
    yrc <- yrpc %>%
      group_by(year, reporter_iso, product_code) %>%
      summarise_trade() %>%
      ungroup()
    
    yrc <- yrc %>% 
      rowwise() %>% 
      mutate(trade_balance = sum(export_value_usd, import_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      filter(trade_balance > 0) %>% 
      select(-trade_balance)
    
    yrc <- yrc %>% 
      left_join(rca_exp, by = c("reporter_iso" = "country_iso", "product_code")) %>%
      left_join(rca_imp, by = c("reporter_iso" = "country_iso", "product_code")) %>%
      select(year, reporter_iso, product_code, everything())
    
    fwrite(yrc, yrc_csv[[t]])
    compress_gz(yrc_csv[[t]])
    rm(yrc)
  }

  # YR -------------------------------------------------------------------

  if (!file.exists(yr_gz[t])) {
    eci_f <- eci_f %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>%
      rename(
        eci_fitness_method = eci,
        eci_rank_fitness_method = eci_rank
      )

    eci_r <- eci_r %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>%
      rename(
        eci_reflections_method = eci,
        eci_rank_reflections_method = eci_rank
      )

    eci_e <- eci_e %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>%
      rename(
        eci_eigenvalues_method = eci,
        eci_rank_eigenvalues_method = eci_rank
      )
    
    max_exp <- yrpc %>%
      group_by(reporter_iso, product_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_export_product_code = product_code,
        top_export_trade_value_usd = export_value_usd
      )

    max_imp <- yrpc %>%
      group_by(reporter_iso, product_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_import_product_code = product_code,
        top_import_trade_value_usd = import_value_usd
      )

    yr <- yrpc %>%
      group_by(year, reporter_iso) %>%
      summarise_trade() %>%
      ungroup()
    
    yr <- yr %>% 
      rowwise() %>% 
      mutate(trade_balance = sum(export_value_usd, import_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      filter(trade_balance > 0) %>% 
      select(-trade_balance)
    
    yr <- yr %>% 
      left_join(eci_f, by = c("reporter_iso" = "country")) %>%
      left_join(eci_r, by = c("reporter_iso" = "country")) %>%
      left_join(eci_e, by = c("reporter_iso" = "country")) %>%
      left_join(max_exp, by = "reporter_iso") %>%
      left_join(max_imp, by = "reporter_iso")
    
    fwrite(yr, yr_csv[t])
    compress_gz(yr_csv[[t]])
    rm(
      yr,
      eci_f, eci_r, eci_e,
      max_exp, max_imp
    )
  }

  # YC -------------------------------------------------------------------

  if (!file.exists(yc_gz[t])) {
    pci_f <- pci_f %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>%
      rename(
        pci_fitness_method = pci,
        pci_rank_fitness_method = pci_rank
      )

    pci_r <- pci_r %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>%
      rename(
        pci_reflections_method = pci,
        pci_rank_reflections_method = pci_rank
      )

    pci_e <- pci_e %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>%
      rename(
        pci_eigenvalues_method = pci,
        pci_rank_eigenvalues_method = pci_rank
      )

    max_exp_2 <- yrpc %>%
      group_by(reporter_iso, product_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(product_code) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_exporter_iso = reporter_iso,
        top_exporter_trade_value_usd = export_value_usd
      )

    max_imp_2 <- yrpc %>%
      group_by(reporter_iso, product_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(product_code) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_importer_iso = reporter_iso,
        top_importer_trade_value_usd = import_value_usd
      )

    yc <- yrpc %>%
      group_by(year, product_code) %>%
      summarise_trade() %>%
      ungroup()
    
    yc <- yc %>% 
      rowwise() %>% 
      mutate(trade_balance = sum(export_value_usd, import_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      filter(trade_balance > 0) %>% 
      select(-trade_balance)
    
    yc <- yc %>% 
      left_join(pci_f, by = c("product_code" = "product")) %>%
      left_join(pci_r, by = c("product_code" = "product")) %>%
      left_join(pci_e, by = c("product_code" = "product")) %>%
      left_join(max_exp_2, by = "product_code") %>%
      left_join(max_imp_2, by = "product_code") %>%
      select(year, product_code, export_value_usd, import_value_usd, starts_with("pci_"), everything())
    
    fwrite(yc, yc_csv[[t]])
    compress_gz(yc_csv[[t]])
  }
}
