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
  
  credentials <- menu(
    c("yes", "no"),
    title = "Have you stored the host, user password and DB name safely in your .Renviron file?",
    graphics = F
  )
  
  stopifnot(credentials == 1)
  
  # helpers -----------------------------------------------------------------
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  # source("00-scripts/04-download-raw-data.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  # source("00-scripts/06-tidy-downloaded-data.R")
  # source("00-scripts/07-convert-tidy-data-codes.R")
  # source("00-scripts/08-join-converted-datasets.R")
  # source("00-scripts/09-compute-rca-and-related-metrics.R")
  # source("00-scripts/10-create-final-tables.R")
  
  # connection parameters ---------------------------------------------------
  
  drv <- dbDriver("PostgreSQL") # choose the driver
  
  dbusr <- Sys.getenv("dbusr")
  dbpwd <- Sys.getenv("dbpwd")
  dbhost <- Sys.getenv("dbhost")
  dbname <- Sys.getenv("dbname")
  
  con <- dbConnect(
    drv,
    host = dbhost,
    port = 5432,
    user = dbusr,
    password = dbpwd,
    dbname = dbname
  )
  
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
    dbWriteTable(con, "attributes_communities", attributes_communities, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # data --------------------------------------------------------------------
  
  lapply(
    seq_along(years_full),
    function(t) {
      yrpc <- fread2(
        yrpc_gz[t],
        character = "product_code",
        numeric = c(
          "export_value_usd",
          "import_value_usd",
          "export_value_usd_change_1_year",
          "export_value_usd_change_5_years",
          "export_value_usd_percentage_change_1_year",
          "export_value_usd_percentage_change_5_years",
          "import_value_usd_change_1_year",
          "import_value_usd_change_5_years",
          "import_value_usd_percentage_change_1_year",
          "import_value_usd_percentage_change_5_years"
        )
      )
      dbWriteTable(con, "hs07_yrpc", yrpc, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(yrpc)
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
      yrp <- fread2(
        yrp_gz[t],
        numeric = c(
          "export_value_usd",
          "import_value_usd",
          "export_value_usd_change_1_year",
          "export_value_usd_change_5_years",
          "export_value_usd_percentage_change_1_year",
          "export_value_usd_percentage_change_5_years",
          "import_value_usd_change_1_year",
          "import_value_usd_change_5_years",
          "import_value_usd_percentage_change_1_year",
          "import_value_usd_percentage_change_5_years"
        )
      )
      dbWriteTable(con, "hs07_yrp", yrp, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(yrp)
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
      yrc <- fread2(
        yrc_gz[t],
        character = "product_code",
        numeric = c(
          "export_value_usd",
          "import_value_usd",
          "export_rca_4_digits_product_code",
          "export_rca_6_digits_product_code",
          "import_rca_4_digits_product_code",
          "import_rca_6_digits_product_code",
          "export_value_usd_change_1_year",
          "export_value_usd_change_5_years",
          "export_value_usd_percentage_change_1_year",
          "export_value_usd_percentage_change_5_years",
          "import_value_usd_change_1_year",
          "import_value_usd_change_5_years",
          "import_value_usd_percentage_change_1_year",
          "import_value_usd_percentage_change_5_years"
        )
      )
      dbWriteTable(con, "hs07_yrc", yrc, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(yrc)
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
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
          "top_import_trade_value_usd",
          "export_value_usd_change_1_year",
          "export_value_usd_change_5_years",
          "export_value_usd_percentage_change_1_year",
          "export_value_usd_percentage_change_5_years",
          "import_value_usd_change_1_year",
          "import_value_usd_change_5_years",
          "import_value_usd_percentage_change_1_year",
          "import_value_usd_percentage_change_5_years"
        )
      )
      dbWriteTable(con, "hs07_yr", yr, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(yr)
    }
  )
  
  lapply(
    seq_along(years_full),
    function(t) {
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
          "top_importer_trade_value_usd",
          "export_value_usd_change_1_year",
          "export_value_usd_change_5_years",
          "export_value_usd_percentage_change_1_year",
          "export_value_usd_percentage_change_5_years",
          "import_value_usd_change_1_year",
          "import_value_usd_change_5_years",
          "import_value_usd_percentage_change_1_year",
          "import_value_usd_percentage_change_5_years"
        )
      )
      dbWriteTable(con, "hs07_yc", yc, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(yc)
    }
  )
}

copy_to_db()
