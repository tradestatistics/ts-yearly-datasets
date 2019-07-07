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
  # PCI/ECI data ------------------------------------------------------------
  
  eci_f <- fread2("05-metrics/hs-rev2007-eci/eci-fitness-joined-ranking.csv.gz")
  eci_r <- fread2("05-metrics/hs-rev2007-eci/eci-reflections-joined-ranking.csv.gz")
  eci_e <- fread2("05-metrics/hs-rev2007-eci/eci-eigenvalues-joined-ranking.csv.gz")
  
  pci_f <- fread2("05-metrics/hs-rev2007-pci/pci-fitness-joined-ranking.csv.gz", character = c("product"))
  pci_r <- fread2("05-metrics/hs-rev2007-pci/pci-reflections-joined-ranking.csv.gz", character = c("product"))
  pci_e <- fread2("05-metrics/hs-rev2007-pci/pci-eigenvalues-joined-ranking.csv.gz", character = c("product"))
  
  # YRPC ------------------------------------------------------------------
  
  if (!file.exists(yrpc_internal_gz[t])) {
    message(paste("Creating YRPC table for the year", years_full[t]))
    
    exports_t1 <- fread2(unified_gz[t], character = "product_code") %>% 
      rename(export_value_usd = trade_value_usd) %>% 
      select(-product_code_length)
    
    imports_t1 <- exports_t1 %>% select(-year)
    names(imports_t1) <- c("partner_iso", "reporter_iso", "product_code", "import_value_usd")
    
    yrpc_t1 <- full_join(exports_t1, imports_t1, by = c("reporter_iso", "partner_iso", "product_code"))
    rm(exports_t1, imports_t1)
    
    if (t %in% match(years_missing_t_minus_1, years_full)) {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t2 = NA,
          import_value_usd_t2 = NA
        )
    } else {
      exports_t2 <- fread2(unified_gz[t - 1], character = "product_code") %>% 
        rename(export_value_usd_t2 = trade_value_usd) %>% 
        select(-c(year, product_code_length))
      
      imports_t2 <- exports_t2
      names(imports_t2) <- c("partner_iso", "reporter_iso", "product_code", "import_value_usd_t2")
      
      yrpc_t2 <- full_join(exports_t2, imports_t2, by = c("reporter_iso", "partner_iso", "product_code"))
      rm(exports_t2, imports_t2)
      
      yrpc_t1 <- yrpc_t1 %>% 
        left_join(yrpc_t2, by = c("reporter_iso", "partner_iso", "product_code"))
      
      rm(yrpc_t2)
    }
    
    if (t %in% match(years_missing_t_minus_5, years_full) | t %in% match(years_missing_t_minus_1, years_full)) {
      yrpc_t1 <- yrpc_t1 %>%
        mutate(
          export_value_usd_t3 = NA,
          import_value_usd_t3 = NA
        )
    } else {
      exports_t3 <- fread2(unified_gz[t - 5], character = "product_code") %>% 
        rename(export_value_usd_t3 = trade_value_usd) %>% 
        select(-c(year, product_code_length))
      
      imports_t3 <- exports_t3
      names(imports_t3) <- c("partner_iso", "reporter_iso", "product_code", "import_value_usd_t3")
      
      yrpc_t3 <- full_join(exports_t3, imports_t3, by = c("reporter_iso", "partner_iso", "product_code"))
      rm(exports_t3, imports_t3)
      
      yrpc_t1 <- yrpc_t1 %>% 
        left_join(yrpc_t3, by = c("reporter_iso", "partner_iso", "product_code"))
      
      rm(yrpc_t3)
    }
    
    yrpc_t1 <- yrpc_t1 %>%
      mutate(
        year = years_full[t],
        product_code_length = str_length(product_code)
      )
    
    fwrite(yrpc_t1, yrpc_internal_csv[t])
    compress_gz(yrpc_internal_csv[t])
    
    yrpc <- yrpc_t1 %>% 
      compute_changes() %>% 
      select(year, matches("iso"), matches("product"), export_value_usd, import_value_usd, matches("change"))
    
    fwrite(yrpc, yrpc_csv[[t]])
    compress_gz(yrpc_csv[[t]])
    rm(yrpc)
  } else {
    message(paste("Reading YRPC table for the year", years_full[t]))
    
    yrpc_t1 <- fread2(
      yrpc_internal_gz[t],
      character = "product_code",
      numeric = c(
        "export_value_usd",
        "import_value_usd",
        "export_value_usd_t2",
        "import_value_usd_t2",
        "export_value_usd_t3",
        "import_value_usd_t3"
      )
    )
    
    if (!file.exists(yrpc_gz[t])) {
      yrpc <- yrpc_t1 %>% 
        compute_changes() %>% 
        select(year, matches("iso"), matches("product"), export_value_usd, import_value_usd, matches("change"))
      
      fwrite(yrpc, yrpc_csv[[t]])
      compress_gz(yrpc_csv[[t]])
      rm(yrpc)
    }
  }
  
  # YRP ------------------------------------------------------------------
  
  if (!file.exists(yrp_gz[t])) {
    yrp <- yrpc_t1 %>%
      filter(product_code_length == 4) %>% 
      group_by(year, reporter_iso, partner_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrp, yrp_csv[[t]])
    compress_gz(yrp_csv[[t]])
    rm(yrp)
  }
  
  # YRC ------------------------------------------------------------------
  
  if (!file.exists(yrc_gz[t])) {
    rca_exp <- fread2(rca_exports_gz[[t]], character = "product_code") %>%
      select(-year)
    
    rca_exp_4 <- rca_exp %>% 
      filter(str_length(product_code) == 4) %>% 
      rename(export_rca_4_digits_product_code = export_rca)
    
    rca_exp_6 <- rca_exp %>% 
      filter(str_length(product_code) == 6) %>% 
      rename(export_rca_6_digits_product_code = export_rca)
    
    rm(rca_exp)
    
    rca_imp <- fread2(rca_imports_gz[[t]], character = "product_code") %>%
      select(-year)
    
    rca_imp_4 <- rca_imp %>% 
      filter(str_length(product_code) == 4) %>% 
      rename(import_rca_4_digits_product_code = import_rca)
    
    rca_imp_6 <- rca_imp %>% 
      filter(str_length(product_code) == 6) %>% 
      rename(import_rca_6_digits_product_code = import_rca)
    
    rm(rca_imp)
    
    yrc <- yrpc_t1 %>%
      group_by(year, reporter_iso, product_code) %>%
      summarise_trade() %>%
      ungroup() %>%
      mutate(product_code_length = str_length(product_code)) %>% 
      left_join(rca_exp_4, by = c("reporter_iso" = "country_iso", "product_code")) %>%
      left_join(rca_exp_6, by = c("reporter_iso" = "country_iso", "product_code")) %>%
      left_join(rca_imp_4, by = c("reporter_iso" = "country_iso", "product_code")) %>%
      left_join(rca_imp_6, by = c("reporter_iso" = "country_iso", "product_code")) %>%
      compute_changes() %>% 
      select(year, reporter_iso, product_code, product_code_length, everything()) %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrc, yrc_csv[[t]])
    compress_gz(yrc_csv[[t]])
    rm(yrc)
  }
  
  # YR -------------------------------------------------------------------
  
  if (!file.exists(yr_gz[t])) {
    eci_f_t1 <- eci_f %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>% 
      rename(
        eci_fitness_method = eci,
        eci_rank_fitness_method = eci_rank
      )
    
    eci_r_t1 <- eci_r %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>% 
      rename(
        eci_reflections_method = eci,
        eci_rank_reflections_method = eci_rank
      )
    
    eci_e_t1 <- eci_f %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>% 
      rename(
        eci_eigenvalues_method = eci,
        eci_rank_eigenvalues_method = eci_rank
      )
    
    if (t %in% match(years_missing_t_minus_1, years_full)) {
      eci_f_t2 <- eci_f_t1 %>%
        select(country) %>% 
        mutate(
          eci_fitness_method_t2 = NA,
          eci_rank_fitness_method_t2 = NA
        )
      
      eci_r_t2 <- eci_r_t1 %>%
        select(country) %>% 
        mutate(
          eci_reflections_method_t2 = NA,
          eci_rank_reflections_method_t2 = NA
        )
      
      eci_e_t2 <- eci_e_t1 %>%
        select(country) %>% 
        mutate(
          eci_eigenvalues_method_t2 = NA,
          eci_rank_eigenvalues_method_t2 = NA
        )
    } else {
      eci_f_t2 <- eci_f %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year) %>% 
        rename(
          eci_fitness_method_t2 = eci,
          eci_rank_fitness_method_t2 = eci_rank
        )
      
      eci_r_t2 <- eci_r %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year) %>% 
        rename(
          eci_reflections_method_t2 = eci,
          eci_rank_reflections_method_t2 = eci_rank
        )
      
      eci_e_t2 <- eci_f %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year) %>% 
        rename(
          eci_eigenvalues_method_t2 = eci,
          eci_rank_eigenvalues_method_t2 = eci_rank
        )
    }
    
    if (any(t %in% match(years_missing_t_minus_1, years_full) | t %in% match(years_missing_t_minus_5, years_full))) {
      eci_f_t3 <- eci_f_t1 %>%
        select(country) %>% 
        mutate(
          eci_fitness_method_t3 = NA,
          eci_rank_fitness_method_t3 = NA
        )
      
      eci_r_t3 <- eci_r_t1 %>%
        select(country) %>% 
        mutate(
          eci_reflections_method_t3 = NA,
          eci_rank_reflections_method_t3 = NA
        )
      
      eci_e_t3 <- eci_e_t1 %>%
        select(country) %>% 
        mutate(
          eci_eigenvalues_method_t3 = NA,
          eci_rank_eigenvalues_method_t3 = NA
        )
    } else {
      eci_f_t3 <- eci_f %>%
        filter(year == years_full[[t - 5]]) %>%
        select(-year) %>% 
        rename(
          eci_fitness_method_t3 = eci,
          eci_rank_fitness_method_t3 = eci_rank
        )
      
      eci_r_t3 <- eci_r %>%
        filter(year == years_full[[t - 5]]) %>%
        select(-year) %>% 
        rename(
          eci_reflections_method_t3 = eci,
          eci_rank_reflections_method_t3 = eci_rank
        )
      
      eci_e_t3 <- eci_f %>%
        filter(year == years_full[[t - 5]]) %>%
        select(-year) %>% 
        rename(
          eci_eigenvalues_method_t3 = eci,
          eci_rank_eigenvalues_method_t3 = eci_rank
        )
    }
    
    max_exp <- yrpc_t1 %>%
      filter(product_code_length == 4) %>% 
      group_by(reporter_iso, product_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_export_product_code = product_code,
        top_export_trade_value_usd = export_value_usd
      )
    
    max_imp <- yrpc_t1 %>%
      filter(product_code_length == 4) %>% 
      group_by(reporter_iso, product_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_import_product_code = product_code,
        top_import_trade_value_usd = import_value_usd
      )
    
    yr <- yrpc_t1 %>%
      filter(product_code_length == 4) %>% 
      group_by(year, reporter_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      
      left_join(eci_f_t1, by = c("reporter_iso" = "country")) %>%
      left_join(eci_f_t2, by = c("reporter_iso" = "country")) %>%
      left_join(eci_f_t3, by = c("reporter_iso" = "country")) %>%
      
      left_join(eci_r_t1, by = c("reporter_iso" = "country")) %>%
      left_join(eci_r_t2, by = c("reporter_iso" = "country")) %>%
      left_join(eci_r_t3, by = c("reporter_iso" = "country")) %>%
      
      left_join(eci_e_t1, by = c("reporter_iso" = "country")) %>%
      left_join(eci_e_t2, by = c("reporter_iso" = "country")) %>%
      left_join(eci_e_t3, by = c("reporter_iso" = "country")) %>%
      
      mutate(
        eci_rank_delta_1_year_fitness_method = eci_rank_fitness_method - eci_rank_fitness_method_t2,
        eci_rank_delta_5_years_fitness_method = eci_rank_fitness_method - eci_rank_fitness_method_t3,
        
        eci_rank_delta_1_year_reflections_method = eci_rank_reflections_method - eci_rank_reflections_method_t2,
        eci_rank_delta_5_years_reflections_method = eci_rank_reflections_method - eci_rank_reflections_method_t3,
        
        eci_rank_delta_1_year_eigenvalues_method = eci_rank_eigenvalues_method - eci_rank_eigenvalues_method_t2,
        eci_rank_delta_5_years_eigenvalues_method = eci_rank_eigenvalues_method - eci_rank_eigenvalues_method_t3
      ) %>%
      
      left_join(max_exp, by = "reporter_iso") %>%
      left_join(max_imp, by = "reporter_iso") %>%
      
      compute_changes() %>% 
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yr, yr_csv[t])
    compress_gz(yr_csv[[t]])
    rm(yr,
       eci_f, eci_r, eci_e,
       eci_f_t1, eci_f_t2, eci_f_t3,
       eci_r_t1, eci_r_t2, eci_r_t3,
       eci_e_t1, eci_e_t2, eci_e_t3,
       max_exp, max_imp)
  }
  
  # YC -------------------------------------------------------------------

  if (!file.exists(yc_gz[t])) {
    pci_f_t1 <- pci_f %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>% 
      rename(
        pci_fitness_method = pci,
        pci_rank_fitness_method = pci_rank
      )
    
    pci_r_t1 <- pci_r %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>% 
      rename(
        pci_reflections_method = pci,
        pci_rank_reflections_method = pci_rank
      )
    
    pci_e_t1 <- pci_e %>%
      filter(year == years_full[[t]]) %>%
      select(-year) %>% 
      rename(
        pci_eigenvalues_method = pci,
        pci_rank_eigenvalues_method = pci_rank
      )
    
    if (t %in% match(years_missing_t_minus_1, years_full)) {
      pci_f_t2 <- pci_f_t1 %>%
        select(product) %>% 
        mutate(
          pci_fitness_method_t2 = NA,
          pci_rank_fitness_method_t2 = NA
        )
      
      pci_r_t2 <- pci_r_t1 %>%
        select(product) %>% 
        mutate(
          pci_reflections_method_t2 = NA,
          pci_rank_reflections_method_t2 = NA
        )
      
      pci_e_t2 <- pci_e_t1 %>%
        select(product) %>% 
        mutate(
          pci_eigenvalues_method_t2 = NA,
          pci_rank_eigenvalues_method_t2 = NA
        )
    } else {
      pci_f_t2 <- pci_f %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year) %>% 
        rename(
          pci_fitness_method_t2 = pci,
          pci_rank_fitness_method_t2 = pci_rank
        )
      
      pci_r_t2 <- pci_r %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year) %>% 
        rename(
          pci_reflections_method_t2 = pci,
          pci_rank_reflections_method_t2 = pci_rank
        )
      
      pci_e_t2 <- pci_e %>%
        filter(year == years_full[[t - 1]]) %>%
        select(-year) %>% 
        rename(
          pci_eigenvalues_method_t2 = pci,
          pci_rank_eigenvalues_method_t2 = pci_rank
        )
    }
    
    if (t %in% match(years_missing_t_minus_1, years_full) | t %in% match(years_missing_t_minus_5, years_full)) {
      pci_f_t3 <- pci_f_t1 %>%
        select(product) %>% 
        mutate(
          pci_fitness_method_t3 = NA,
          pci_rank_fitness_method_t3 = NA
        )
      
      pci_r_t3 <- pci_r_t1 %>%
        select(product) %>% 
        mutate(
          pci_reflections_method_t3 = NA,
          pci_rank_reflections_method_t3 = NA
        )
      
      pci_e_t3 <- pci_e_t1 %>%
        select(product) %>% 
        mutate(
          pci_eigenvalues_method_t3 = NA,
          pci_rank_eigenvalues_method_t3 = NA
        )
    } else {
      pci_f_t3 <- pci_f %>%
        filter(year == years_full[[t - 5]]) %>%
        select(-year) %>% 
        rename(
          pci_fitness_method_t3 = pci,
          pci_rank_fitness_method_t3 = pci_rank
        )
      
      pci_r_t3 <- pci_r %>%
        filter(year == years_full[[t - 5]]) %>%
        select(-year) %>% 
        rename(
          pci_reflections_method_t3 = pci,
          pci_rank_reflections_method_t3 = pci_rank
        )
      
      pci_e_t3 <- pci_e %>%
        filter(year == years_full[[t - 5]]) %>%
        select(-year) %>% 
        rename(
          pci_eigenvalues_method_t3 = pci,
          pci_rank_eigenvalues_method_t3 = pci_rank
        )
    }
    
    max_exp_2 <- yrpc_t1 %>%
      group_by(reporter_iso, product_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(product_code) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_exporter_iso = reporter_iso,
        top_exporter_trade_value_usd = export_value_usd
      )
    
    max_imp_2 <- yrpc_t1 %>%
      group_by(reporter_iso, product_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(product_code) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_importer_iso = reporter_iso,
        top_importer_trade_value_usd = import_value_usd
      )
    
    yc <- yrpc_t1 %>%
      group_by(year, product_code) %>%
      summarise_trade() %>%
      ungroup() %>% 
      mutate(product_code_length = str_length(product_code)) %>% 
      
      left_join(pci_f_t1, by = c("product_code" = "product")) %>%
      left_join(pci_f_t2, by = c("product_code" = "product")) %>%
      left_join(pci_f_t3, by = c("product_code" = "product")) %>%

      left_join(pci_r_t1, by = c("product_code" = "product")) %>%
      left_join(pci_r_t2, by = c("product_code" = "product")) %>%
      left_join(pci_r_t3, by = c("product_code" = "product")) %>%
      
      left_join(pci_e_t1, by = c("product_code" = "product")) %>%
      left_join(pci_e_t2, by = c("product_code" = "product")) %>%
      left_join(pci_e_t3, by = c("product_code" = "product")) %>%
      
      mutate(
        pci_rank_delta_1_year_fitness_method = pci_rank_fitness_method - pci_rank_fitness_method_t2,
        pci_rank_delta_5_years_fitness_method = pci_rank_fitness_method - pci_rank_fitness_method_t3,
        
        pci_rank_delta_1_year_reflections_method = pci_rank_reflections_method - pci_rank_reflections_method_t2,
        pci_rank_delta_5_years_reflections_method = pci_rank_reflections_method - pci_rank_reflections_method_t3,
        
        pci_rank_delta_1_year_eigenvalues_method = pci_rank_eigenvalues_method - pci_rank_eigenvalues_method_t2,
        pci_rank_delta_5_years_eigenvalues_method = pci_rank_eigenvalues_method - pci_rank_eigenvalues_method_t3
      ) %>%
      
      left_join(max_exp_2, by = "product_code") %>%
      left_join(max_imp_2, by = "product_code") %>%
      
      compute_changes() %>%
      select(year, product_code, product_code_length, export_value_usd, import_value_usd, starts_with("pci_"), everything()) %>% 
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yc, yc_csv[[t]])
    compress_gz(yc_csv[[t]])
  }
}
