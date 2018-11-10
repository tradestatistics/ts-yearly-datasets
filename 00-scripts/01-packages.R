# Open ts-yearly-data.Rproj before running this function

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[['sysname']]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  pacman::p_load(data.table, jsonlite, dplyr, tidyr, stringr, janitor, purrr, rlang, Matrix, RPostgreSQL, doParallel)
} else {
  pacman::p_load(data.table, jsonlite, dplyr, tidyr, stringr, janitor, purrr, rlang, Matrix, RPostgreSQL)
}
