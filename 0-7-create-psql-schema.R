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
    
    # Countries ---------------------------------------------------------------

    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_countries")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.attributes_countries
      (
      country_iso varchar(3) DEFAULT '' PRIMARY KEY NOT NULL,
      country_name_english varchar(255) DEFAULT NULL,
      country_fullname_english varchar(255) DEFAULT NULL,
      continent_id integer DEFAULT NULL,
      continent varchar(255) DEFAULT NULL,
      eu28_member integer DEFAULT NULL
      )"
    )
    
    # Product names -----------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_products")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.attributes_products 
      (
      product_code varchar(6) DEFAULT '' PRIMARY KEY NOT NULL,
      product_fullname_english varchar(255) DEFAULT NULL,
      group_code varchar(2) DEFAULT NULL,
      group_name varchar(255) DEFAULT NULL
      )"
    )
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_products_shortnames")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.attributes_products_shortnames 
      (
      product_code varchar(4) NOT NULL,
      product_shortname_english varchar(255) DEFAULT NULL,
      CONSTRAINT hs07_attributes_products_shortnames_pk PRIMARY KEY (product_code),
      CONSTRAINT hs07_attributes_products_shortnames_attributes_products_pk FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
    
    # Communities -------------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_communities")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.attributes_communities 
      (
      product_code varchar(6) NOT NULL,
      community_code varchar(2) DEFAULT NULL,
      community_name varchar(255) DEFAULT NULL,
      community_colour varchar(7) DEFAULT NULL,
      CONSTRAINT hs07_attributes_communities_pk PRIMARY KEY (product_code),
      CONSTRAINT hs07_attributes_communities_attributes_products_fk FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
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
      eci_4_digits_product_code float DEFAULT NULL,
      eci_rank_4_digits_product_code integer DEFAULT NULL,
      eci_rank_4_digits_product_code_delta_1_year integer DEFAULT NULL,
      eci_rank_4_digits_product_code_delta_5_years integer DEFAULT NULL,
      top_export_product_code varchar(6) DEFAULT NULL,
      top_export_trade_value_usd decimal(16,2) DEFAULT NULL,
      top_import_product_code varchar(6) DEFAULT NULL,
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
      CONSTRAINT hs07_yr_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso)
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
      CONSTRAINT hs07_yrp_attributes_countries_id_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrp_attributes_countries_id_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso)
      )"
    )
    
    # Year - Reporter - Partner - Product Code --------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yrpc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yrpc 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      partner_iso varchar(3) NOT NULL,
      product_code varchar(6) NOT NULL,
      product_code_length integer DEFAULT NULL,
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
      CONSTRAINT hs07_yrpc_pk PRIMARY KEY (year, reporter_iso, partner_iso, product_code),
      CONSTRAINT hs07_yrpc_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrpc_attributes_countries_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrpc_attributes_product_names_fk_3 FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
    
    # Year - Reporter - Product Code ------------------------------------------

    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yrc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yrc 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      product_code varchar(6) NOT NULL,
      product_code_length integer DEFAULT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_rca_4_digits_product_code float DEFAULT NULL,
      export_rca_6_digits_product_code float DEFAULT NULL,
      import_rca_4_digits_product_code float DEFAULT NULL,
      import_rca_6_digits_product_code float DEFAULT NULL,
      export_value_usd_change_1_year decimal DEFAULT NULL,
      export_value_usd_change_5_years decimal DEFAULT NULL,
      export_value_usd_percentage_change_1_year float DEFAULT NULL,
      export_value_usd_percentage_change_5_years float DEFAULT NULL,
      import_value_usd_change_1_year decimal DEFAULT NULL,
      import_value_usd_change_5_years decimal DEFAULT NULL,
      import_value_usd_percentage_change_1_year float DEFAULT NULL,
      import_value_usd_percentage_change_5_years float DEFAULT NULL,
      CONSTRAINT hs07_yrc_pk PRIMARY KEY (year, reporter_iso, product_code),
      CONSTRAINT hs07_yrc_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrc_attributes_product_names_fk_2 FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
    
    # Year - Product Code -----------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yc 
      (
      year integer NOT NULL,
      product_code varchar(6) NOT NULL,
      product_code_length integer DEFAULT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      pci_4_digits_product_code float DEFAULT NULL,
      pci_rank_4_digits_product_code integer DEFAULT NULL,
      pci_6_digits_product_code float DEFAULT NULL,
      pci_rank_6_digits_product_code integer DEFAULT NULL,
      pci_rank_4_digits_product_code_delta_1_year integer DEFAULT NULL,
      pci_rank_6_digits_product_code_delta_1_year integer DEFAULT NULL,
      pci_rank_4_digits_product_code_delta_5_years integer DEFAULT NULL,
      pci_rank_6_digits_product_code_delta_5_years integer DEFAULT NULL,
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
      CONSTRAINT hs07_yc_pk PRIMARY KEY (year, product_code),
      CONSTRAINT hs07_yc_attributes_product_names_fk FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
  }
  }

create_schema()
