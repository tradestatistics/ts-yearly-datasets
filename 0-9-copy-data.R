# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")
p_load(data.table, dplyr, stringr, janitor, RPostgreSQL)

# helpers -----------------------------------------------------------------

source("0-0-helpers.R")

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

# replace 55 by seq_along(yrpc_gz)

overwrite = F

for (t in 54:40) {
  yrpc <- fread2(yrpc_gz[[t]], char = c("commodity_code"))
  dbWriteTable(con, "hs07_yrpc", yrpc, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(yrpc)
  
  yrp <- fread2(yrp_gz[[t]])
  dbWriteTable(con, "hs07_yrp", yrp, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(yrp)
  
  yrc <- fread2(yrc_gz[[t]], char = c("commodity_code"))
  dbWriteTable(con, "hs07_yrc", yrc, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(yrc)
  
  ypc <- fread2(ypc_gz[[t]], char = c("commodity_code"))
  dbWriteTable(con, "hs07_ypc", ypc, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(ypc)
  
  yr <- fread2(yr_gz[[t]])
  dbWriteTable(con, "hs07_yr", yr, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(yr)
  
  yp <- fread2(yp_gz[[t]])
  dbWriteTable(con, "hs07_yp", yp, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(yp)
  
  yc <- fread2(yc_gz[[t]], char = c("commodity_code"))
  dbWriteTable(con, "hs07_yc", yc, append = TRUE, overwrite = overwrite, row.names = FALSE)
  rm(yc)
}
