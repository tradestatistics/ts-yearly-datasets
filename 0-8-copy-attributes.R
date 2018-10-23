# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
p_load(data.table, dplyr, stringr, janitor, RPostgreSQL)

# helpers -----------------------------------------------------------------

source("0-0-helpers.R")

copy_attributes <- function(overwrite = F) {
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
  
  message("Copying attributes...")
  
  # Countries attributes ----------------------------------------------------
  
  obs_attributes_countries <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM attributes_countries"))
  
  if (obs_attributes_countries == 0) {
    attributes_countries <- fread2(paste0(tables_dir, "/attributes_countries.csv.gz"))
    dbWriteTable(con, "attributes_countries", attributes_countries, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # Products attributes -----------------------------------------------------
  
  obs_attributes_products <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM attributes_products"))
  
  if (obs_attributes_products == 0) {
    attributes_products <- fread2(paste0(tables_dir, "/attributes_products.csv.gz"), char = c("commodity_code", "group_code"))
    dbWriteTable(con, "attributes_products", attributes_products, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
}

copy_attributes()
