# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

# user input --------------------------------------------------------------

if (exists("ask_for_token")) {
  has_token <- menu(
    c("yes", "no"),
    title = "Have you safely stored your token in your .Rprofile file?",
    graphics = F
  )

  stopifnot(has_token == 1)
}

if (exists("ask_for_db_access")) {
  credentials <- menu(
    c("yes", "no"),
    title = "Have you stored the host, user password and DB name safely in your .Renviron file?",
    graphics = F
  )
  
  stopifnot(credentials == 1)
}

dataset <- menu(
  c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
  title = "Select dataset:",
  graphics = F
)

if (exists("ask_convert")) {
  dataset2 <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
    title = "Convert to:",
    graphics = F
  )  
}

if (exists("ask_to_remove_old_files")) {
  remove_old_files <- menu(
    c("yes", "no"),
    title = "Remove old files (y/n):",
    graphics = F
  )
}

if (exists("ask_to_remove_from_db")) {
  remove_from_db <- menu(
    c("yes", "no"),
    title = "Remove outdated records from DB (y/n):",
    graphics = F
  )
}

if (exists("ask_number_of_cores")) {
  n_cores <- readline(prompt = "How many cores to use?: ")
}
