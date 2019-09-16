# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

clean <- function() {
  # messages ----------------------------------------------------------------

  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
This file is part of Open Trade Statistics project.
The scripts within this project are released under GNU General Public License 3.0.\n
This program is free software and comes with ABSOLUTELY NO WARRANTY.
You are welcome to redistribute it under certain conditions.
See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")

  # scripts -----------------------------------------------------------------

  ask_number_of_cores <<- 1
  
  source("00-scripts/00-user-input-and-derived-classification-digits-years.R")
  source("00-scripts/01-packages.R")
  source("00-scripts/02-dirs-and-files.R")
  source("00-scripts/03-misc.R")
  source("00-scripts/05-read-extract-remove-compress.R")
  source("00-scripts/06-tidy-downloaded-data.R")

  # create tidy datasets ----------------------------------------------------

  messageline()
  message("Rearranging files. Please wait...")

  if (operating_system != "Windows") {
    mclapply(seq_along(raw_zip), compute_tidy_data, mc.cores = n_cores)
  } else {
    lapply(seq_along(raw_zip), compute_tidy_data)
  }
}

clean()
