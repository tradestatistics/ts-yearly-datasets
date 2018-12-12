# Open oec-yearly-data.Rproj before running this function

copy_attributes <- function(overwrite = F) {
  # messages ----------------------------------------------------------------
  
  message("\nCopyright (C) 2018, Mauricio \"Pacha\" Vargas\n")
  message("This file is part of Open Trade Statistics project")
  message("\nThe scripts within this project are released under GNU General Public License 3.0")
  message("This program comes with ABSOLUTELY NO WARRANTY.")
  message("This is free software, and you are welcome to redistribute it under certain conditions.\n")
  message("See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details\n")
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
  # Rcpp::sourceCpp("00-scripts/09-proximity-countries-denominator.cpp")
  # Rcpp::sourceCpp("00-scripts/10-proximity-products-denominator.cpp")
  # source("00-scripts/11-compute-rca-and-related-metrics.R")
  # source("00-scripts/12-create-final-tables.R")
  
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
  
  obs_attributes_countries <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_country_names"))
  
  if (obs_attributes_countries == 0) {
    attributes_countries <- fread2(paste0(tables_dir, "/attributes_countries.csv.gz"))
    dbWriteTable(con, "attributes_country_names", attributes_countries, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # products attributes
  
  obs_attributes_products <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_product_names"))
  
  if (obs_attributes_products == 0) {
    attributes_products <- fread2(paste0(tables_dir, "/attributes_products.csv.gz"), character = c("commodity_code", "group_code", "community_code"))
    dbWriteTable(con, "attributes_product_names", attributes_products, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # data --------------------------------------------------------------------

  lapply(
    seq_along(years_full),
    function (t) {
      yrpc <- fread2(
        yrpc_gz[[t]], 
        character = "commodity_code",
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
      
      yrp <- fread2(
        yrp_gz[[t]],
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
      
      yrc <- fread2(
        yrc_gz[[t]], 
        character = "commodity_code",
        numeric = c(
          "export_value_usd",
          "import_value_usd",
          "export_rca_4_digits_commodity_code",
          "export_rca_6_digits_commodity_code",
          "import_rca_4_digits_commodity_code",
          "import_rca_6_digits_commodity_code",
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
      
      ypc <- fread2(
        ypc_gz[[t]], 
        character = "commodity_code",
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
      dbWriteTable(con, "hs07_ypc", ypc, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(ypc)
      
      yr <- fread2(
        yr_gz[[t]], 
        character = c("top_export_commodity_code", "top_import_commodity_code"), 
        numeric = c(
          "export_value_usd",
          "import_value_usd",
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
      
      yp <- fread2(
        yp_gz[[t]],
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
      dbWriteTable(con, "hs07_yp", yp, append = TRUE, overwrite = overwrite, row.names = FALSE)
      rm(yp)
      
      yc <- fread2(
        yc_gz[[t]], 
        character = "commodity_code",
        numeric = c(
          "export_value_usd",
          "import_value_usd",
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

copy_attributes()
