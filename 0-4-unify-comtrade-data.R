unify <- function(n_cores = 4) {
  # detect system -----------------------------------------------------------

  operating_system <- Sys.info()[["sysname"]]

  # packages ----------------------------------------------------------------

  if (!require("pacman")) install.packages("pacman")

  if (operating_system != "Windows") {
    pacman::p_load(data.table, dplyr, tidyr, stringr, doParallel)
  } else {
    pacman::p_load(data.table, dplyr, tidyr, stringr)
  }

  # helpers -----------------------------------------------------------------

  source("0-0-helpers.R")

  # user parameters ---------------------------------------------------------

  message(
    "This function takes data obtained from UN Comtrade by using download functions in this project and creates tidy datasets"
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

  # input dir ---------------------------------------------------------------

  clean_dir <- "02-clean-data-comtrade/hs-rev2007"

  converted_dir <- "03-converted-data-comtrade"

  # output dir --------------------------------------------------------------

  unified_dir <- "03-unified-data-comtrade"
  try(dir.create(unified_dir))

  # input files -------------------------------------------------------------

  clean_files <- list.files(clean_dir, pattern = "gz", recursive = T, full.names = T)

  converted_files <- list.files(converted_dir, pattern = "gz", recursive = T, full.names = T)

  all_files <- c(clean_files, converted_files)

  years <- 1962:2016

  # convert data ------------------------------------------------------------

  try(dir.create(paste0(unified_dir, "/hs-rev2007")))

  unified_gz_files <- paste0(unified_dir, "/hs-rev2007/hs-rev2007-", years, ".csv.gz")

  unified_csv_files <- str_replace(unified_gz_files, ".gz", "")

  fill_gaps <- function(x, y, z, t) {
    if (!file.exists(grep(paste(years[[t]], ".csv.gz", sep = ""), z, value = T))) {
      if (years[[t]] < 1976) {
        d1 <- grep(paste(years[[t]], ".csv.gz", sep = ""), x, value = T)
        d2 <- NULL
        d3 <- NULL
        d4 <- NULL
        d5 <- NULL
      }

      if (years[[t]] >= 1976 & years[[t]] < 1992) {
        d1 <- grep(paste("sitc-rev2-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d2 <- grep(paste("sitc-rev1-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d3 <- NULL
        d4 <- NULL
        d5 <- NULL
      }

      if (years[[t]] >= 1992 & years[[t]] < 1996) {
        d1 <- grep(paste("hs-rev1992-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d2 <- grep(paste("sitc-rev2-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d3 <- NULL
        d4 <- NULL
        d5 <- NULL
      }

      if (years[[t]] >= 1996 & years[[t]] < 2002) {
        d1 <- grep(paste("hs-rev1996-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d2 <- grep(paste("hs-rev1992-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d3 <- grep(paste("sitc-rev2-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d4 <- NULL
        d5 <- NULL
      }

      if (years[[t]] >= 2002 & years[[t]] < 2007) {
        d1 <- grep(paste("hs-rev2002-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d2 <- grep(paste("hs-rev1996-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d3 <- grep(paste("hs-rev1992-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d4 <- grep(paste("sitc-rev2-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d5 <- NULL
      }

      if (years[[t]] >= 2007) {
        d1 <- grep(paste("hs-rev2007-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d2 <- grep(paste("hs-rev2002-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d3 <- grep(paste("hs-rev1996-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d4 <- grep(paste("hs-rev1992-", years[[t]], ".csv.gz", sep = ""), x, value = T)
        d5 <- grep(paste("sitc-rev2-", years[[t]], ".csv.gz", sep = ""), x, value = T)
      }

      data <- fread3(d1) %>%
        unite(pairs, reporter_iso, partner_iso)

      if (!is.null(d2)) {
        data2 <- fread3(d2) %>%
          unite(pairs, reporter_iso, partner_iso) %>%
          anti_join(data, by = "pairs")
      } else {
        data2 <- NULL
      }

      if (!is.null(d3)) {
        data3 <- fread3(d3) %>%
          unite(pairs, reporter_iso, partner_iso) %>%
          anti_join(data, by = "pairs") %>%
          anti_join(data2, by = "pairs")
      }

      if (!is.null(d4)) {
        data4 <- fread3(d4) %>%
          unite(pairs, reporter_iso, partner_iso) %>%
          anti_join(data, by = "pairs") %>%
          anti_join(data2, by = "pairs") %>%
          anti_join(data3, by = "pairs")
      }

      if (!is.null(d5)) {
        data5 <- fread3(d5) %>%
          unite(pairs, reporter_iso, partner_iso) %>%
          anti_join(data, by = "pairs") %>%
          anti_join(data2, by = "pairs") %>%
          anti_join(data3, by = "pairs") %>%
          anti_join(data4, by = "pairs")
      }

      if (!is.null(d2) & !is.null(d3) & is.null(d4) & is.null(d5)) {
        data2 <- bind_rows(data2, data3)
        rm(data3)
      }

      if (!is.null(d2) & !is.null(d3) & !is.null(d4) & is.null(d5)) {
        data2 <- bind_rows(data2, data3, data4)
        rm(data3, data4)
      }

      if (!is.null(d2) & !is.null(d3) & !is.null(d4) & !is.null(d5)) {
        data2 <- bind_rows(data2, data3, data4, data5)
        rm(data3, data4, data5)
      }

      if (!is.null(d2)) {
        data <- bind_rows(data, data2) %>%
          separate(pairs, c("reporter_iso", "partner_iso")) %>%
          arrange(reporter_iso, partner_iso, commodity_code)
        
        rm(data2) 
      }

      fwrite(data, y[[t]])
      compress_gz(y[[t]])
    }
  }

  if (operating_system != "Windows") {
    mclapply(seq_along(years), fill_gaps,
      mc.cores = n_cores,
      x = all_files, y = unified_csv_files, z = unified_gz_files
    )
  } else {
    lapply(seq_along(years), fill_gaps,
      x = all_files, y = unified_csv_files, z = unified_gz_files
    )
  }
}

unify()
