# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

create_schema <- function(overwrite = F) {
  # messages ----------------------------------------------------------------
  
  message("\nCopyright (C) 2018, Mauricio \"Pacha\" Vargas\n")
  message("This file is part of Open Trade Statistics project")
  message("\nThe scripts within this project are released under GNU General Public License 3.0")
  message("This program comes with ABSOLUTELY NO WARRANTY.")
  message("This is free software, and you are welcome to redistribute it under certain conditions.\n")
  message("See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details\n")
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")
  
  credentials <- menu(c("yes", "no"),
                      title = "Have you stored the host, user password and DB name safely in your .Renviron file?",
                      graphics = F)
  
  stopifnot(credentials == 1)
  
  # helpers -----------------------------------------------------------------
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  # source("00-scripts/04-download-raw-data.R")
  # source("00-scripts/05-read-extract-remove-compress.R")
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
  
  # List tables associated with the public schema
  db_tables <- dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
  
  if (nrow(db_tables) > 0 & overwrite == F) {
    messageline()
    message("The DB schema is not empty so it won't be replaced.")
  } else {
    messageline()
    message("Creating/Overwriting DB schema...")
    
    # Country names -----------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_country_names")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.attributes_country_names
      (
      country_iso varchar(3) DEFAULT '' PRIMARY KEY NOT NULL,
      country_name_english varchar(255) DEFAULT NULL,
      country_fullname_english varchar(255) DEFAULT NULL,
      country_abbreviation varchar(255) DEFAULT NULL
      )"
    )
    
    # Product names -----------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_product_names")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.attributes_product_names 
      (
      commodity_code varchar(6) DEFAULT '' PRIMARY KEY NOT NULL,
      product_fullname_english varchar(255) DEFAULT NULL,
      group_code varchar(2) DEFAULT NULL,
      group_name varchar(255) DEFAULT NULL,
      community_code varchar(2) DEFAULT NULL,
      community_name varchar(255) DEFAULT NULL,
      colour varchar(7) DEFAULT NULL
      )"
    )
    
    # Year - Partner ----------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yp")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yp 
      (
      year integer NOT NULL,
      partner_iso varchar(3) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yp_pk PRIMARY KEY (year, partner_iso),
      CONSTRAINT hs07_yp_attributes_country_names_fk FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso)
      )"
    )
    
    # Year - Partner - Commodity ----------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_ypc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_ypc 
      (
      year integer NOT NULL,
      partner_iso varchar(3) NOT NULL,
      commodity_code varchar(6) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_ypc_pk PRIMARY KEY (year, partner_iso, commodity_code),
      CONSTRAINT hs07_ypc_attributes_country_names_fk FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso),
      CONSTRAINT hs07_ypc_attributes_product_names_fk_2 FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
      )"
    )
    
    # Year - Reporter ---------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yr")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yr 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      top_export_commodity_code varchar(6) DEFAULT NULL,
      top_export_trade_value_usd decimal(16,2) DEFAULT NULL,
      top_import_commodity_code varchar(6) DEFAULT NULL,
      top_import_trade_value_usd decimal(16,2) DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yr_pk PRIMARY KEY (year, reporter_iso),
      CONSTRAINT hs07_yr_attributes_country_names_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso)
      )"
    )
    
    # Year - Reporter - Partner -----------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yrp")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yrp 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      partner_iso varchar(3) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yrp_pk PRIMARY KEY (year, reporter_iso, partner_iso),
      CONSTRAINT hs07_yrp_attributes_country_names_id_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso),
      CONSTRAINT hs07_yrp_attributes_country_names_id_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso)
      )"
    )
    
    # Year - Reporter - Partner - Commodity -----------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yrpc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yrpc 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      partner_iso varchar(3) NOT NULL,
      commodity_code varchar(6) NOT NULL,
      commodity_code_length integer DEFAULT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yrpc_pk PRIMARY KEY (year, reporter_iso, partner_iso, commodity_code),
      CONSTRAINT hs07_yrpc_attributes_country_names_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso),
      CONSTRAINT hs07_yrpc_attributes_country_names_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_country_names (country_iso),
      CONSTRAINT hs07_yrpc_attributes_product_names_fk_3 FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
      )"
    )
    
    # Year - Reporter - Commodity ---------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yrc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yrc 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      commodity_code varchar(6) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_rca_4_digits_commodity_code float DEFAULT NULL,
      export_rca_6_digits_commodity_code float DEFAULT NULL,
      import_rca_4_digits_commodity_code float DEFAULT NULL,
      import_rca_6_digits_commodity_code float DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yrc_pk PRIMARY KEY (year, reporter_iso, commodity_code),
      CONSTRAINT hs07_yrc_attributes_country_names_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_country_names (country_iso),
      CONSTRAINT hs07_yrc_attributes_product_names_fk_2 FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
      )"
    )
    
    # Year - Commodity --------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yc 
      (
      year integer NOT NULL,
      commodity_code varchar(6) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      pci_4_digits_commodity_code float DEFAULT NULL,
      pci_6_digits_commodity_code float DEFAULT NULL,
      pci_rank_4_digits_commodity_code integer DEFAULT NULL,
      pci_rank_6_digits_commodity_code integer DEFAULT NULL,
      pci_rank_4_digits_commodity_code_delta integer DEFAULT NULL,
      pci_rank_6_digits_commodity_code_delta integer DEFAULT NULL,
      top_exporter_iso varchar(3) DEFAULT NULL,
      top_exporter_trade_value_usd decimal(16,2) DEFAULT NULL,
      top_importer_iso varchar(3) DEFAULT NULL,
      top_importer_trade_value_usd decimal(16,2) DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yc_pk PRIMARY KEY (year, commodity_code),
      CONSTRAINT hs07_yc_attributes_product_names_fk FOREIGN KEY (commodity_code) REFERENCES public.attributes_product_names (commodity_code)
      )"
    )
  }
  }

create_schema()
