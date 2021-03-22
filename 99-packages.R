# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[["sysname"]]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  pacman::p_load(
    data.table, jsonlite, tibble, dplyr, tidyr, stringr, janitor, purrr, rlang,
    economiccomplexity, Matrix, RPostgres, glue, doParallel
  )
} else {
  pacman::p_load(
    data.table, jsonlite, tibble, dplyr, tidyr, stringr, janitor, purrr, rlang,
    economiccomplexity, Matrix, RPostgres, glue
  )
}
