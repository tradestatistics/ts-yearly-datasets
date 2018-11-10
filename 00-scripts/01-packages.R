# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[['sysname']]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  pacman::p_load(data.table, jsonlite, dplyr, tidyr, stringr, janitor, purrr, rlang, Matrix, RPostgreSQL, doParallel)
} else {
  pacman::p_load(data.table, jsonlite, dplyr, tidyr, stringr, janitor, purrr, rlang, Matrix, RPostgreSQL)
}
