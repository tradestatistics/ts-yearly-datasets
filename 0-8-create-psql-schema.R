# Open ts-yearly-data.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

create_schema <- function(overwrite = F) {
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
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")
  
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
      product_code varchar(4) DEFAULT '' PRIMARY KEY NOT NULL,
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
      product_code varchar(4) NOT NULL,
      community_code varchar(2) DEFAULT NULL,
      community_name varchar(255) DEFAULT NULL,
      community_color varchar(7) DEFAULT NULL,
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
      eci_fitness_method float DEFAULT NULL,
      eci_rank_fitness_method integer DEFAULT NULL,
      eci_reflections_method float DEFAULT NULL,
      eci_rank_reflections_method integer DEFAULT NULL,
      eci_eigenvalues_method float DEFAULT NULL,
      eci_rank_eigenvalues_method integer DEFAULT NULL,
      top_export_product_code varchar(4) DEFAULT NULL,
      top_export_trade_value_usd decimal(16,2) DEFAULT NULL,
      top_import_product_code varchar(4) DEFAULT NULL,
      top_import_trade_value_usd decimal(16,2) DEFAULT NULL,
      CONSTRAINT hs07_yr_pk PRIMARY KEY (year, reporter_iso),
      CONSTRAINT hs07_yr_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso)
      )"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yr_idx_y ON public.hs07_yr (year)"
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
      CONSTRAINT hs07_yrp_pk PRIMARY KEY (year, reporter_iso, partner_iso),
      CONSTRAINT hs07_yrp_attributes_countries_id_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrp_attributes_countries_id_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso)
      )"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrp_idx_y ON public.hs07_yrp (year)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrp_idx_yr ON public.hs07_yrp (year, reporter_iso)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrp_idx_yp ON public.hs07_yrp (year, partner_iso)"
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
      product_code varchar(4) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      CONSTRAINT hs07_yrpc_pk PRIMARY KEY (year, reporter_iso, partner_iso, product_code),
      CONSTRAINT hs07_yrpc_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrpc_attributes_countries_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrpc_attributes_product_names_fk_3 FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrpc_idx_y ON public.hs07_yrpc (year)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrpc_idx_yr ON public.hs07_yrpc (year, reporter_iso)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrpc_idx_yp ON public.hs07_yrpc (year, partner_iso)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrpc_idx_yrp ON public.hs07_yrpc (year, reporter_iso, partner_iso)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrpc_idx_yrc ON public.hs07_yrpc (year, reporter_iso, product_code)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrpc_idx_ypc ON public.hs07_yrpc (year, partner_iso, product_code)"
    )
    
    # Year - Reporter - Product Code ------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yrc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yrc 
      (
      year integer NOT NULL,
      reporter_iso varchar(3) NOT NULL,
      product_code varchar(4) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      export_rca float DEFAULT NULL,
      import_rca float DEFAULT NULL,
      CONSTRAINT hs07_yrc_pk PRIMARY KEY (year, reporter_iso, product_code),
      CONSTRAINT hs07_yrc_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
      CONSTRAINT hs07_yrc_attributes_product_names_fk_2 FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrc_idx_y ON public.hs07_yrc (year)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrc_idx_yr ON public.hs07_yrc (year, reporter_iso)"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yrc_idx_yc ON public.hs07_yrc (year, product_code)"
    )
    
    # Year - Product Code -----------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.hs07_yc")
    
    dbSendQuery(
      con,
      "CREATE TABLE public.hs07_yc
      (
      year integer NOT NULL,
      product_code varchar(4) NOT NULL,
      export_value_usd decimal(16,2) DEFAULT NULL,
      import_value_usd decimal(16,2) DEFAULT NULL,
      pci_fitness_method float DEFAULT NULL,
      pci_rank_fitness_method integer DEFAULT NULL,
      pci_reflections_method float DEFAULT NULL,
      pci_rank_reflections_method integer DEFAULT NULL,
      pci_eigenvalues_method float DEFAULT NULL,
      pci_rank_eigenvalues_method integer DEFAULT NULL,
      top_exporter_iso varchar(3) DEFAULT NULL,
      top_exporter_trade_value_usd decimal(16,2) DEFAULT NULL,
      top_importer_iso varchar(3) DEFAULT NULL,
      top_importer_trade_value_usd decimal(16,2) DEFAULT NULL,
      CONSTRAINT hs07_yc_pk PRIMARY KEY (year, product_code),
      CONSTRAINT hs07_yc_attributes_product_names_fk FOREIGN KEY (product_code) REFERENCES public.attributes_products (product_code)
      )"
    )
    
    dbSendQuery(
      con,
      "CREATE INDEX CONCURRENTLY hs07_yc_idx_y ON public.hs07_yc (year)"
    )
  }
}

create_schema()
