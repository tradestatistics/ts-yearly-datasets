# Open ts-yearly-data.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

tables <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # helpers -----------------------------------------------------------------

  ask_number_of_cores <<- 1
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")
  
  # functions ---------------------------------------------------------------

  summarise_trade <- function(d) {
    d %>%
      summarise(
        export_value_usd = sum(export_value_usd, na.rm = T),
        import_value_usd = sum(import_value_usd, na.rm = T)
      )
  }
  
  compute_tables <- function(t) {
    # PCI/ECI data ------------------------------------------------------------
    
    eci_f <- readRDS("05-metrics/hs-rev2007-eci/eci-fitness-joined-ranking.rds")
    eci_r <- readRDS("05-metrics/hs-rev2007-eci/eci-reflections-joined-ranking.rds")
    eci_e <- readRDS("05-metrics/hs-rev2007-eci/eci-eigenvalues-joined-ranking.rds")
    
    pci_f <- readRDS("05-metrics/hs-rev2007-pci/pci-fitness-joined-ranking.rds")
    pci_r <- readRDS("05-metrics/hs-rev2007-pci/pci-reflections-joined-ranking.rds")
    pci_e <- readRDS("05-metrics/hs-rev2007-pci/pci-eigenvalues-joined-ranking.rds")
    
    # YRPC ------------------------------------------------------------------
    
    if (!file.exists(yrpc_rds[t])) {
      message(paste("Creating YRPC table for the year", years_full[t]))
      
      exports <- readRDS(unified_rds[t]) %>%
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
      
      saveRDS(yrpc, file = yrpc_rds[[t]], compress = "xz")
    } else {
      message(paste("Reading YRPC table for the year", years_full[t]))
      
      yrpc <- readRDS(yrpc_rds[t])
    }
    
    # YRP ------------------------------------------------------------------
    
    if (!file.exists(yrp_rds[t])) {
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
      
      saveRDS(yrp, file = yrp_rds[[t]], compress = "xz")
      rm(yrp)
    }
    
    # YRC ------------------------------------------------------------------
    
    if (!file.exists(yrc_rds[t])) {
      rca_exp <- readRDS(rca_exports_rds[[t]]) %>%
        select(-year)
      
      rca_imp <- readRDS(rca_imports_rds[[t]]) %>%
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
      
      saveRDS(yrc, file = yrc_rds[[t]], compress = "xz")
      rm(yrc)
    }
    
    # YR -------------------------------------------------------------------
    
    if (!file.exists(yr_rds[t])) {
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
        filter(product_code != "9999") %>% 
        group_by(reporter_iso, product_code) %>%
        summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
        group_by(reporter_iso) %>%
        slice(which.max(export_value_usd)) %>%
        rename(
          top_export_product_code = product_code,
          top_export_trade_value_usd = export_value_usd
        ) %>% 
        filter(top_export_trade_value_usd > 0)
      
      max_imp <- yrpc %>%
        filter(product_code != "9999") %>% 
        group_by(reporter_iso, product_code) %>%
        summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
        group_by(reporter_iso) %>%
        slice(which.max(import_value_usd)) %>%
        rename(
          top_import_product_code = product_code,
          top_import_trade_value_usd = import_value_usd
        ) %>% 
        filter(top_import_trade_value_usd > 0)
      
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
      
      saveRDS(yr, file = yr_rds[t], compress = "xz")
      
      rm(
        yr,
        eci_f, eci_r, eci_e,
        max_exp, max_imp
      )
    }
    
    # YC -------------------------------------------------------------------
    
    if (!file.exists(yc_rds[t])) {
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
        filter(product_code != "9999") %>% 
        group_by(reporter_iso, product_code) %>%
        summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
        group_by(product_code) %>%
        slice(which.max(export_value_usd)) %>%
        rename(
          top_exporter_iso = reporter_iso,
          top_exporter_trade_value_usd = export_value_usd
        ) %>% 
        filter(top_exporter_trade_value_usd > 0)
      
      max_imp_2 <- yrpc %>%
        filter(product_code != "9999") %>% 
        group_by(reporter_iso, product_code) %>%
        summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
        group_by(product_code) %>%
        slice(which.max(import_value_usd)) %>%
        rename(
          top_importer_iso = reporter_iso,
          top_importer_trade_value_usd = import_value_usd
        ) %>% 
        filter(top_importer_trade_value_usd > 0)
      
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
      
      saveRDS(yc, file = yc_rds[[t]], compress = "xz")
    }
  }
  
  # codes -------------------------------------------------------------------

  load("../comtrade-codes/01-2-tidy-country-data/country-codes.RData")
  load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")

  load("../observatory-codes/02-2-product-data-tidy/hs-rev2007-product-names.RData")
  hs_product_names_07 <- hs_product_names

  load("../observatory-codes/02-2-product-data-tidy/hs-rev1992-product-names.RData")
  
  hs_product_names_92 <- hs_product_names %>%
    select(hs, product_name) %>%
    rename(
      product_code = hs,
      product_shortname_english = product_name
    )

  rm(hs_product_names)

  # input data --------------------------------------------------------------

  attributes_countries <- country_codes %>%
    select(
      iso3_digit_alpha, contains("name"), country_abbrevation,
      contains("continent"), eu28_member
    ) %>%
    rename(
      country_iso = iso3_digit_alpha,
      country_abbreviation = country_abbrevation
    ) %>%
    mutate(country_iso = str_to_lower(country_iso)) %>%
    filter(country_iso != "null") %>%
    distinct(country_iso, .keep_all = T) %>%
    select(-country_abbreviation)

  if (!file.exists(paste0(attributes_dir, "/attributes_countries.rds"))) {
    saveRDS(attributes_countries, file = paste0(attributes_dir, "/attributes_countries.rds"), compress = "xz")
  }

  product_names <- product_codes %>%
    filter(classification == "H3", str_length(code) %in% c(4, 6)) %>%
    select(code, description) %>%
    rename(
      product_code = code,
      product_fullname_english = description
    ) %>%
    mutate(group_code = str_sub(product_code, 1, 2))

  product_names_2 <- product_codes %>%
    filter(classification == "H3", str_length(code) == 2) %>%
    select(code, description) %>%
    rename(
      group_code = code,
      group_name = description
    )

  product_names_3 <- product_names %>%
    left_join(hs_product_names_07, by = c("product_code" = "hs")) %>%
    rename(
      community_code = group_id,
      community_name = group_name
    ) %>%
    select(product_code, community_code, community_name)

  colors <- product_names_3 %>%
    select(community_code) %>%
    distinct() %>%
    mutate(community_color = c(
      "#74c0e2", "#406662", "#549e95", "#8abdb6", "#bcd8af",
      "#a8c380", "#ede788", "#d6c650", "#dc8e7a", "#d05555",
      "#bf3251", "#872a41", "#993f7b", "#7454a6", "#a17cb0",
      "#d1a1bc", "#a1aafb", "#5c57d9", "#1c26b3", "#4d6fd0",
      "#7485aa", "#635b56"
    ))

  attributes_products <- product_names %>%
    left_join(product_names_2)

  rm(product_names_2)

  if (!file.exists(paste0(attributes_dir, "/attributes_products.rds"))) {
    saveRDS(attributes_products, file = paste0(attributes_dir, "/attributes_products.rds"), compress = "xz")
  }

  attributes_communities <- product_names_3 %>%
    left_join(colors)

  if (!file.exists(paste0(attributes_dir, "/attributes_communities.rds"))) {
    saveRDS(attributes_communities, file = paste0(attributes_dir, "/attributes_communities.rds"), compress = "xz")
  }

  rm(product_names_3)

  attributes_products_shortnames <- attributes_products %>%
    select(product_code, product_fullname_english) %>%
    filter(str_length(product_code) == 4) %>%
    left_join(hs_product_names_92)

  attributes_products_shortnames_complete <- attributes_products_shortnames %>%
    filter(!is.na(product_shortname_english))

  attributes_products_shortnames_nas <- attributes_products_shortnames %>%
    filter(is.na(product_shortname_english)) %>%
    mutate(
      product_shortname_english = c(
        "Mercury-based compounds",
        "Miscellaneous inorganic products",
        "Chemical mixtures",
        "Chemical waste",
        "Ovine prepared leather",
        "Non-Ovine prepared leather",
        "Chamois leather",
        "Composition leather",
        "Knitted fabrics, width <= 30 cms",
        "Kniteed fabrics, width > 30 cms",
        "Warp knit fabrics",
        "Other fabrics",
        "Semiconductor machines",
        "Non-electric machinery"
      )
    )

  attributes_products_shortnames <- attributes_products_shortnames_complete %>%
    bind_rows(attributes_products_shortnames_nas) %>%
    mutate(
      product_shortname_english = iconv(product_shortname_english, from = "", to = "UTF-8"),
      product_shortname_english = ifelse(product_code == "0903", "Mate", product_shortname_english)
    ) %>%
    arrange(product_code) %>%
    select(-product_fullname_english)

  if (!file.exists(paste0(attributes_dir, "/attributes_products_shortnames.rds"))) {
    saveRDS(attributes_products_shortnames, file = paste0(attributes_dir, "/attributes_products_shortnames.rds"), compress = "xz")
  }

  rm(attributes_products_shortnames_complete, attributes_products_shortnames_nas)

  # tables ------------------------------------------------------------------

  if (operating_system != "Windows") {
    mclapply(seq_along(years_full), compute_tables, mc.cores = n_cores)
  } else {
    lapply(seq_along(years_full), compute_tables)
  }
}

tables()
