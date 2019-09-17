# Open ts-yearly-data.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

copy_to_db <- function(overwrite = F) {
  # messages ----------------------------------------------------------------
  
  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
          This file is part of Open Trade Statistics project.
          The scripts within this project are released under GNU General Public License 3.0.\n
          This program is free software and comes with ABSOLUTELY NO WARRANTY.
          You are welcome to redistribute it under certain conditions.
          See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")
  
  # scripts -----------------------------------------------------------------
  
  ask_for_db_access <<- 1
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  
  # attributes --------------------------------------------------------------
  
  message("Copying attributes...")
  
  # countries attributes
  
  obs_attributes_countries <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_countries"))
  
  if (obs_attributes_countries == 0) {
    attributes_countries <- fread2(paste0(tables_dir, "/0-attributes/attributes_countries.csv.gz"))
    dbWriteTable(con, "attributes_countries", attributes_countries, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # products attributes
  
  obs_attributes_products <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_products"))
  
  if (obs_attributes_products == 0) {
    attributes_products <- fread2(paste0(tables_dir, "/0-attributes/attributes_products.csv.gz"), character = c("product_code", "group_code"))
    attributes_products <- filter(attributes_products, str_length(product_code) == 4)
    dbWriteTable(con, "attributes_products", attributes_products, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  obs_attributes_products_shortnames <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_products_shortnames"))
  
  if (obs_attributes_products_shortnames == 0) {
    attributes_products_shortnames <- fread2(paste0(tables_dir, "/0-attributes/attributes_products_shortnames.csv.gz"), character = "product_code")
    dbWriteTable(con, "attributes_products_shortnames", attributes_products_shortnames, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # communities attributes
  
  obs_attributes_communities <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_communities"))
  
  if (obs_attributes_communities == 0) {
    attributes_communities <- fread2(paste0(tables_dir, "/0-attributes/attributes_communities.csv.gz"), character = c("product_code", "community_code"))
    attributes_communities <- filter(attributes_communities, str_length(product_code) == 4)
    dbWriteTable(con, "attributes_communities", attributes_communities, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # data --------------------------------------------------------------------
  
  lapply(
    seq_along(years_full),
    function(t) {
      obs_yrpc <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yrpc WHERE year = %s", years_full[t])))
      
      if (obs_yrpc == 0) {
        message(sprintf("Copying year %s to table YRPC...", years_full[t]))
        yrpc <- fread2(
          yrpc_gz[t],
          character = "product_code",
          numeric = c(
            "export_value_usd",
            "import_value_usd"
          )
        )
        
        dbWriteTable(con, "hs07_yrpc", yrpc, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yrpc)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YRPC.", years_full[t]))
      }
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
      obs_yrp <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yrp WHERE year = %s", years_full[t])))
      
      if (obs_yrp == 0) {
        message(sprintf("Copying year %s to table YRP...", years_full[t]))
        
        yrp <- fread2(
          yrp_gz[t],
          numeric = c(
            "export_value_usd",
            "import_value_usd"
          )
        )
        
        dbWriteTable(con, "hs07_yrp", yrp, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yrp)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YRP.", years_full[t]))
      }
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
      obs_yrc <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yrc WHERE year = %s", years_full[t])))
      
      if (obs_yrc == 0) {
        message(sprintf("Copying year %s to table YRP...", years_full[t]))
        
        yrc <- fread2(
          yrc_gz[t],
          character = "product_code",
          numeric = c(
            "export_value_usd",
            "import_value_usd",
            "export_rca",
            "import_rca"
          )
        )
        
        dbWriteTable(con, "hs07_yrc", yrc, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yrc)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YRC.", years_full[t]))
      }
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
      obs_yr <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yr WHERE year = %s", years_full[t])))
      
      if (obs_yr == 0) {
        message(sprintf("Copying year %s to table YR...", years_full[t]))
        
        yr <- fread2(
          yr_gz[t],
          character = c("top_export_product_code", "top_import_product_code"),
          numeric = c(
            "export_value_usd",
            "import_value_usd",
            "eci_fitness_method",
            "eci_reflections_method",
            "eci_eigenvalues_method",
            "top_export_trade_value_usd",
            "top_import_trade_value_usd"
          )
        )
        
        dbWriteTable(con, "hs07_yr", yr, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yr)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YR.", years_full[t]))
      }
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
      obs_yc <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yc WHERE year = %s", years_full[t])))
      
      if (obs_yc == 0) {
        yc <- fread2(
          yc_gz[t],
          character = "product_code",
          numeric = c(
            "export_value_usd",
            "import_value_usd",
            "pci_fitness_method",
            "pci_reflections_method",
            "pci_eigenvalues_method",
            "top_exporter_trade_value_usd",
            "top_importer_trade_value_usd"
          )
        )
        
        dbWriteTable(con, "hs07_yc", yc, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yc)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YC.", years_full[t]))
      }
    }
  )
}

copy_to_db()
