# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
p_load(stringr, RPostgreSQL)

# helpers -----------------------------------------------------------------

source("0-0-helpers.R")

create_schema <- function(overwrite = F) {
  # User parameters ---------------------------------------------------------
  
  message("\nCopyright (c) 2018, Mauricio \"Pacha\" Vargas\n")
  readline(prompt = "Press [enter] to continue")
  message("\nThe MIT License\n")
  message(
    "Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the \"Software\"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:"
  )
  message(
    "\nThe above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software."
  )
  message(
    "\nTHE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.\n"
  )
  readline(prompt = "Press [enter] to continue")
  
  credentials <- menu(
    c("yes", "no"),
    title = "Have you stored the host, user password and DB name safely in your .Renviron file?",
    graphics = F
  )
  
  stopifnot(credentials == 1)
  
  # Settings ----------------------------------------------------------------
  
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
    
    dbSendQuery(con, "DROP TABLE IF EXISTS public.attributes_countries")
    
    dbSendQuery(con, 
                "CREATE TABLE public.attributes_countries (
                  country_iso varchar(3) DEFAULT '' PRIMARY KEY NOT NULL,
                  country_name_english varchar(255) DEFAULT NULL,
                  country_fullname_english varchar(255) DEFAULT NULL,
                  country_abbreviation varchar(255) DEFAULT NULL
                )"
    )
    
    # Product names -----------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS attributes_products")
    
    dbSendQuery(con, 
                "CREATE TABLE attributes_products (
                  commodity_code varchar(4) DEFAULT '' PRIMARY KEY NOT NULL,
                  product_fullname_english varchar(400) DEFAULT NULL,
                  group_code varchar(2) DEFAULT NULL,
                  group_name varchar(255) DEFAULT NULL,
                  color varchar(7) DEFAULT NULL
                )"
    )
    
    # Year - Partner ----------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_yp")
    
    dbSendQuery(con, 
                "CREATE TABLE hs07_yp (
                  year integer NOT NULL,
                  partner_iso varchar(3) NOT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_yp_pk PRIMARY KEY (year, partner_iso),
                  CONSTRAINT hs07_yp_attributes_countries_fk FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso)
                )"
    )
    
    # Year - Partner - Commodity ----------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_ypc")
    
    dbSendQuery(con, 
                "CREATE TABLE hs07_ypc (
                  year integer NOT NULL,
                  partner_iso varchar(3) NOT NULL,
                  commodity_code varchar(4) NOT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_ypc_pk PRIMARY KEY (year, partner_iso, commodity_code),
                  CONSTRAINT hs07_ypc_attributes_countries_fk FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso),
                  CONSTRAINT hs07_ypc_attributes_products_fk_2 FOREIGN KEY (commodity_code) REFERENCES public.attributes_products (commodity_code)
                )"
    )
    
    # Year - Reporter ---------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_yr")
    
    dbSendQuery(con, 
                "CREATE TABLE hs07_yr (
                  year integer NOT NULL,
                  reporter_iso varchar(3) NOT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  top_export_commodity_code varchar(4) DEFAULT NULL,
                  top_export_value_usd decimal DEFAULT NULL,
                  top_import_commodity_code varchar(4) DEFAULT NULL,
                  top_import_value_usd decimal DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_yr_pk PRIMARY KEY (year, reporter_iso),
                  CONSTRAINT hs07_yr_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso)
                )"
    )
    
    # Year - Reporter - Partner -----------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_yrp")
    
    dbSendQuery(con, 
                "CREATE TABLE hs07_yrp (
                  year integer NOT NULL,
                  reporter_iso varchar(3) NOT NULL,
                  partner_iso varchar(3) NOT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_yrp_pk PRIMARY KEY (year, reporter_iso, partner_iso),
                  CONSTRAINT hs07_yrp_attributes_countries_id_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
                  CONSTRAINT hs07_yrp_attributes_countries_id_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso)
                )"
    )
    
    # Year - Reporter - Partner - Commodity -----------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_yrpc")
    
    dbSendQuery(con, 
                "CREATE TABLE hs07_yrpc (
                  year integer NOT NULL,
                  reporter_iso varchar(3) NOT NULL,
                  partner_iso varchar(3) NOT NULL,
                  commodity_code varchar(4) NOT NULL,
                  commodity_code_length integer DEFAULT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_yrpc_pk PRIMARY KEY (year, reporter_iso, partner_iso, commodity_code),
                  CONSTRAINT hs07_yrpc_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
                  CONSTRAINT hs07_yrpc_attributes_countries_fk_2 FOREIGN KEY (partner_iso) REFERENCES public.attributes_countries (country_iso),
                  CONSTRAINT hs07_yrpc_attributes_products_fk_3 FOREIGN KEY (commodity_code) REFERENCES public.attributes_products (commodity_code)
                )"
    )
    
    # Year - Reporter - Commodity ---------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_yrc")
    
    dbSendQuery(con, 
                "CREATE TABLE hs07_yrc (
                  year integer NOT NULL,
                  reporter_iso varchar(3) NOT NULL,
                  commodity_code varchar(4) NOT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  export_rca float DEFAULT NULL,
                  import_rca float DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_yrc_pk PRIMARY KEY (year, reporter_iso, commodity_code),
                  CONSTRAINT hs07_yrc_attributes_countries_fk FOREIGN KEY (reporter_iso) REFERENCES public.attributes_countries (country_iso),
                  CONSTRAINT hs07_yrc_attributes_products_fk_2 FOREIGN KEY (commodity_code) REFERENCES public.attributes_products (commodity_code)
                )"
    )
    
    # Year - Commodity --------------------------------------------------------
    
    dbSendQuery(con, "DROP TABLE IF EXISTS hs07_yc")
    
    dbSendQuery(con,
                "CREATE TABLE hs07_yc (
                  year integer NOT NULL,
                  commodity_code varchar(4) NOT NULL,
                  export_value_usd decimal DEFAULT NULL,
                  import_value_usd decimal DEFAULT NULL,
                  pci float DEFAULT NULL,
                  pci_rank integer DEFAULT NULL,
                  pci_rank_delta integer DEFAULT NULL,
                  top_exporter_iso varchar(3) DEFAULT NULL,
                  top_importer_iso varchar(3) DEFAULT NULL,
                  export_value_usd_change_1year decimal DEFAULT NULL,
                  export_value_usd_change_5years decimal DEFAULT NULL,
                  export_value_usd_percentage_change_1year float DEFAULT NULL,
                  export_value_usd_percentage_change_5years float DEFAULT NULL,
                  import_value_usd_change_1year decimal DEFAULT NULL,
                  import_value_usd_change_5years decimal DEFAULT NULL,
                  import_value_usd_percentage_change_1year float DEFAULT NULL,
                  import_value_usd_percentage_change_5years float DEFAULT NULL,
                  CONSTRAINT hs07_yc_pk PRIMARY KEY (year, commodity_code),
                  CONSTRAINT hs07_yc_attributes_products_fk FOREIGN KEY (commodity_code) REFERENCES public.attributes_products (commodity_code)
                )"
    )
  }
}

create_schema()
