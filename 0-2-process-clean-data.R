# Open oec-yearly-data.Rproj before running this function

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[['sysname']]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  p_load(data.table, feather, dplyr, tidyr, janitor, doParallel)
} else {
  p_load(data.table, feather, dplyr, tidyr, janitor)
}

clean <- function(compress_output = T, n_cores = 4) {
  # user parameters ---------------------------------------------------------

  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets ready to be added to the OEC"
  )
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

  dataset <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 2"),
    title = "Select dataset:",
    graphics = F
  )
  
  # years by classification -------------------------------------------------

  if (dataset < 5) {
    classification <- "hs"
  } else {
    classification <- "sitc"
  }
  if (dataset == 1) {
    rev <- 1992
  }
  if (dataset == 2) {
    rev <- 1996
  }
  if (dataset == 3) {
    rev <- 2002
  }
  if (dataset == 4) {
    rev <- 2007
  }
  if (dataset == 5) {
    rev <- 2
  }

  if (classification == "sitc" & rev == 2) {
    years <- 1990:2016
  }
  if (classification == "hs") {
    years <- rev:2016
  }

  # number of digits --------------------------------------------------------

  J <- ifelse(classification == "sitc", 4, 6)

  # input dirs --------------------------------------------------------------

  raw_dir <- "1-1-raw-data"
  raw_zip_dir <- sprintf("%s/%s-rev%s/zip", raw_dir, classification, rev)
  raw_csv_dir <- sprintf("%s/%s-rev%s/csv", raw_dir, classification, rev)

  # output dirs -------------------------------------------------------------

  clean_dir <- "1-2-clean-data"
  rev_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, rev)
  try(dir.create(clean_dir))
  try(dir.create(rev_dir))

  # helpers -----------------------------------------------------------------

  messageline <- function() {
    message(rep("-", 60))
  }

  extract <- function(t) {
    if (file.exists(raw_csv_list[[t]])) {
      messageline()
      message(paste(raw_zip_list[[t]], "already unzipped. Skipping."))
    } else {
      messageline()
      message(paste("Unzipping", raw_zip_list[[t]]))
      system(sprintf("7z e -aos %s -oc:%s", raw_zip_list[[t]], raw_csv_dir))
    }
  }

  fread2 <- function(x) {
    messageline()
    message("function fread2")
    message("x: ", x)
    as_tibble(fread(
      x,
      colClasses = list(
        character = c("Commodity Code"),
        numeric = c("Trade Value (US$)")
      )
    )) %>% clean_names()
  }

  file.remove2 <- function(x) {
    try(file.remove(x))
  }

  compress <- function(t) {
    if (file.exists(clean_zip_list[[t]])) {
      message("The compressed files exist. Skipping.")
    } else {
      messageline()
      system(paste("7z a", clean_zip_list[[t]], clean_feather_list[[t]]))
      file.remove2(clean_feather_list[[t]])
    }
  }

  # list input files --------------------------------------------------------

  raw_zip_list <-
    list.files(
      path = raw_zip_dir,
      pattern = "\\.zip",
      full.names = T
    ) %>%
    grep(paste(paste0("ps-", years), collapse = "|"), ., value = TRUE)

  raw_csv_list <- raw_zip_list %>% gsub("zip", "csv", .)

  # list output files -------------------------------------------------------

  clean_feather_list <- sprintf("%s/%s-rev%s-%s.feather", rev_dir, classification, rev, years)

  clean_zip_list <- clean_feather_list %>%
    gsub("feather", "zip", .)

  # uncompress input --------------------------------------------------------

  if (operating_system != "Windows") {
    mclapply(1:length(raw_csv_list), extract, mc.cores = n_cores)
  } else {
    lapply(1:length(raw_csv_list), extract)
  }
  # create tidy datasets ----------------------------------------------------

  messageline()
  message("Rearranging files. Please wait...")

  # See Anderson & van Wincoop, 2004, Hummels, 2006 and Gaulier & Zignago, 2010 for 8% consistency
  cif_fob_rate <- 1.08

  compute_tidy_data <- function(t) {
    if (!file.exists(clean_feather_list[[t]])) {
      messageline()
      message(paste("Dividing year", years[[t]]))

      # clean data --------------------------------------------------------------

      clean_feather <- fread2(raw_csv_list[[t]]) %>%
        rename(trade_value_usd = trade_value_us) %>%
        select(trade_flow, reporter_iso, partner_iso, aggregate_level, commodity_code, trade_value_usd) %>%
        filter(aggregate_level %in% J) %>%
        filter(trade_flow == "Export" | trade_flow == "Import") %>%
        filter(
          !is.na(reporter_iso),
          !is.na(partner_iso),
          reporter_iso != "",
          partner_iso != "",
          reporter_iso != " ",
          partner_iso != " "
        ) %>%
        mutate(
          reporter_iso = tolower(reporter_iso),
          partner_iso = tolower(partner_iso)
        ) %>%
        filter(
          reporter_iso != "sxm",
          partner_iso != "sxm", # San Marino is "smr" and not "sxm"
          reporter_iso != "wld",
          partner_iso != "wld" # the World (wld) is not needed as the OEC aggregates and computes total trade
        )

      # exports data ------------------------------------------------------------

      exports <- clean_feather %>%
        filter(trade_flow == "Export") %>%
        unite(pairs, reporter_iso, partner_iso, sep = "_", remove = F) %>%
        rename(export_usd = trade_value_usd) %>%
        select(pairs, commodity_code, export_usd)

      exports_mirrored <- clean_feather %>%
        filter(trade_flow == "Import") %>%
        unite(pairs, partner_iso, reporter_iso, sep = "_", remove = F) %>%
        rename(export_usd_mirrored = trade_value_usd) %>%
        select(pairs, commodity_code, export_usd_mirrored)

      exports_model <- exports %>%
        full_join(exports_mirrored, by = c("pairs", "commodity_code")) %>%
        mutate(export_usd = round(export_usd, 0)) %>%
        mutate(export_usd_mirrored = round(export_usd_mirrored / cif_fob_rate, 0)) %>%
        rowwise() %>%
        mutate(export_usd = max(export_usd, export_usd_mirrored, na.rm = T)) %>%
        select(-matches("mirrored")) %>%
        ungroup()

      rm(exports, exports_mirrored)

      # imports data ------------------------------------------------------------

      imports <- clean_feather %>%
        filter(trade_flow == "Import") %>%
        unite(pairs, reporter_iso, partner_iso, sep = "_", remove = F) %>%
        rename(import_usd = trade_value_usd) %>%
        select(pairs, commodity_code, import_usd)

      imports_mirrored <- clean_feather %>%
        filter(trade_flow == "Export") %>%
        unite(pairs, partner_iso, reporter_iso, sep = "_", remove = F) %>%
        rename(import_usd_mirrored = trade_value_usd) %>%
        select(pairs, commodity_code, import_usd_mirrored)

      imports_model <- imports %>%
        full_join(imports_mirrored, by = c("pairs", "commodity_code")) %>%
        mutate(import_usd = round(import_usd, 0)) %>%
        mutate(import_usd_mirrored = round(import_usd_mirrored / cif_fob_rate, 0)) %>%
        rowwise() %>%
        mutate(import_usd = max(import_usd, import_usd_mirrored, na.rm = T)) %>%
        select(pairs, commodity_code, import_usd) %>%
        ungroup()

      rm(imports, imports_mirrored, clean_feather)

      # trade data --------------------------------------------------------------

      clean_feather <- exports_model %>%
        full_join(imports_model) %>%
        separate(pairs, c("reporter_iso", "partner_iso"), sep = "_") %>%
        mutate(year = years[[t]]) %>%
        select(year, everything())

      write_feather(clean_feather, clean_feather_list[[t]])

      rm(clean_feather)
    } else {
      messageline()
      message(paste("Skipping year", years[[t]], "Files exist."))
    }
    rm(check_files)
  }

  if (operating_system != "Windows") {
    mclapply(1:length(raw_csv_list), compute_tidy_data, mc.cores = n_cores)
  } else {
    lapply(1:length(raw_csv_list), compute_tidy_data)
  }
  
  # compress output ---------------------------------------------------------

  if (compress_output == T) {
    if (operating_system != "Windows") {
      mclapply(1:length(clean_zip_list), compress, mc.cores = n_cores)
    } else {
      lapply(1:length(clean_zip_list), compress)
    }
  }

  unlink(raw_csv_dir, recursive = T)
  rm(raw_zip_list, raw_csv_list, clean_feather_list, clean_zip_list)
}

clean()
