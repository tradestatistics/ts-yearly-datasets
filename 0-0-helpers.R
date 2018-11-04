# Open ts-yearly-data.Rproj before running this function

# user input --------------------------------------------------------------

dataset <- menu(
  c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
  title = "Select dataset:",
  graphics = F
)

# detect system -----------------------------------------------------------

operating_system <- Sys.info()[['sysname']]

# packages ----------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

if (operating_system != "Windows") {
  pacman::p_load(data.table, jsonlite, dplyr, tidyr, stringr, janitor, purrr, rlang, Matrix, RPostgreSQL, doParallel)
} else {
  pacman::p_load(data.table, jsonlite, dplyr, tidyr, stringr, janitor, purrr, rlang, Matrix, RPostgreSQL)
}

# years by classification -------------------------------------------------

if (dataset < 5) {
  classification <- "hs"
} else {
  classification <- "sitc"
}

if (dataset == 1) { revision <- 1992; revision2 <- revision; classification2 <- "H0" }
if (dataset == 2) { revision <- 1996; revision2 <- revision; classification2 <- "H1" }
if (dataset == 3) { revision <- 2002; revision2 <- revision; classification2 <- "H2" }
if (dataset == 4) { revision <- 2007; revision2 <- revision; classification2 <- "H3" }
if (dataset == 5) { revision <- 1; revision2 <- 1962; classification2 <- "S1" }
if (dataset == 6) { revision <- 2; revision2 <- 1976; classification2 <- "S2" }
if (dataset == 7) { revision <- 3; revision2 <- 1988; classification2 <- "S3" }
if (dataset == 8) { revision <- 4; revision2 <- 2007; classification2 <- "S4" }

years <- revision2:2016

years_missing_t_minus_1 <- 1962
years_missing_t_minus_2 <- 1963
years_missing_t_minus_5 <- 1963:1966
years_full <- 1967:2016

years_sitc_rev1 <- 1962:1991 # because SITC rev1 is used just to complete SITC rev2 data

# number of digits --------------------------------------------------------

if (classification == "sitc") {
  J <- c(4,5) 
} else {
  J <- c(4,6)
}

# dirs/files --------------------------------------------------------

# 0-1-download-data.R

raw_dir <- "01-raw-data"
try(dir.create(raw_dir))

raw_dir <- sprintf("%s/%s-rev%s", raw_dir, classification, revision)
try(dir.create(raw_dir))

# 0-2-clean-data.R

raw_zip <- list.files(
  path = raw_dir,
  pattern = "\\.zip",
  full.names = T
) %>%
  grep(paste(paste0("ps-", years), collapse = "|"), ., value = TRUE)

raw_csv <- raw_zip %>% gsub("zip", "csv", .)

clean_dir <- "02-clean-data"
rev_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, revision)
try(dir.create(clean_dir))
try(dir.create(rev_dir))

clean_gz <- sprintf("02-clean-data/%s-rev%s/%s-rev%s-%s.csv.gz", classification, revision, classification, revision, years)
clean_csv <- gsub(".gz", "", clean_gz)

# 0-3-convert-data.R

c1 <- c("hs-rev1992", "hs-rev1996", "hs-rev2002", "hs-rev2007", "sitc-rev1", "sitc-rev2")

c2 <- c("hs92", "hs96", "hs02", "hs07", "sitc1", "sitc2")

converted_dir <- "03-converted-data"
try(dir.create(converted_dir))

# 0-4-unify-data.R

unified_dir <- "03-unified-data"
try(dir.create(unified_dir))

converted_gz <- list.files(converted_dir, pattern = "gz", recursive = T, full.names = T)

converted_csv <- str_replace(converted_gz, ".gz", "")

clean_and_coverted <- c(clean_gz, converted_gz)

try(dir.create(paste0(unified_dir, "/hs-rev2007")))

unified_gz <- paste0(unified_dir, "/hs-rev2007/hs-rev2007-", years, ".csv.gz")

unified_csv <- str_replace(unified_gz, ".gz", "")

# 0-5-compute-metrics.R

metrics_dir <- "04-metrics"
try(dir.create(metrics_dir))

#unified_gz <- list.files(path = unified_dir, full.names = T, recursive = T)

rca_exports_dir <- sprintf("%s/hs-rev2007-rca-exports", metrics_dir)
try(dir.create(rca_exports_dir))

rca_exports_gz <- sprintf("%s/rca-exports-%s.csv.gz", rca_exports_dir, years)
rca_exports_csv <- gsub(".gz", "", rca_exports_gz)

rca_imports_dir <- sprintf("%s/hs-rev2007-rca-imports", metrics_dir)
try(dir.create(rca_imports_dir))

rca_imports_gz <- sprintf("%s/rca-imports-%s.csv.gz", rca_imports_dir, years)
rca_imports_csv <- gsub(".gz", "", rca_imports_gz)

eci_dir <- sprintf("%s/hs-rev2007-eci", metrics_dir)
try(dir.create(eci_dir))
eci_rankings_gz <- sprintf("%s/eci-%s.csv.gz", eci_dir, years)
eci_rankings_csv <- gsub(".gz", "", eci_rankings_gz)

pci_dir <- sprintf("%s/hs-rev2007-pci", metrics_dir)
try(dir.create(pci_dir))
pci_rankings_gz <- sprintf("%s/pci-%s.csv.gz", pci_dir, years)
pci_rankings_csv <- gsub(".gz", "", pci_rankings_gz)

proximity_countries_dir <- sprintf("%s/hs-rev2007-proximity-countries", metrics_dir)
try(dir.create(proximity_countries_dir))
proximity_countries_gz <- sprintf("%s/proximity-countries-%s.csv.gz", proximity_countries_dir, years)
proximity_countries_csv <- gsub(".gz", "", proximity_countries_gz)

proximity_products_dir <- sprintf("%s/hs-rev2007-proximity-products", metrics_dir)
try(dir.create(proximity_products_dir))
proximity_products_gz <- sprintf("%s/proximity-products-%s.csv.gz", proximity_products_dir, years)
proximity_products_csv <- gsub(".gz", "", proximity_products_gz)

density_countries_dir <- sprintf("%s/hs-rev2007-density-countries", metrics_dir)
try(dir.create(density_countries_dir))
density_countries_gz <- sprintf("%s/density-countries-%s.csv.gz", density_countries_dir, years)
density_countries_csv <- gsub(".gz", "", density_countries_gz)

density_products_dir <- sprintf("%s/hs-rev2007-density-products", metrics_dir)
try(dir.create(density_products_dir))
density_products_gz <- sprintf("%s/density-products-%s.csv.gz", density_products_dir, years)
density_products_csv <- gsub(".gz", "", density_products_gz)

# 0-6-create-tables.R

tables_dir <- "05-tables"
try(dir.create(tables_dir))

tables_dir <- sprintf("%s/hs-rev2007", tables_dir)
try(dir.create(tables_dir))

yrpc_dir <- paste0(tables_dir, "/1-yrpc")
try(dir.create(yrpc_dir))

yrp_dir <- paste0(tables_dir, "/2-yrp")
try(dir.create(yrp_dir))

yrc_dir <- paste0(tables_dir, "/3-yrc")
try(dir.create(yrc_dir))

ypc_dir <- paste0(tables_dir, "/4-ypc")
try(dir.create(ypc_dir))

yr_dir <- paste0(tables_dir, "/5-yr")
try(dir.create(yr_dir))

yp_dir <- paste0(tables_dir, "/6-yp")
try(dir.create(yp_dir))

yc_dir <- paste0(tables_dir, "/7-yc")
try(dir.create(yc_dir))

yrpc_gz <- unified_gz %>%
  gsub(unified_dir, yrpc_dir, .) %>% 
  gsub("1-yrpc/hs-rev2007/hs-rev2007", "1-yrpc/yrpc", .)

yrpc_csv <- yrpc_gz %>% gsub(".gz", "", .)

yrp_gz <- unified_gz %>%
  gsub(unified_dir, yrp_dir, .) %>% 
  gsub("2-yrp/hs-rev2007/hs-rev2007", "2-yrp/yrp", .)

yrp_csv <- yrp_gz %>% gsub(".gz", "", .)

yrc_gz <- unified_gz %>%
  gsub(unified_dir, yrc_dir, .) %>% 
  gsub("3-yrc/hs-rev2007/hs-rev2007", "3-yrc/yrc", .)

yrc_csv <- yrc_gz %>% gsub(".gz", "", .)

ypc_gz <- unified_gz %>%
  gsub(unified_dir, ypc_dir, .) %>% 
  gsub("4-ypc/hs-rev2007/hs-rev2007", "4-ypc/ypc", .)

ypc_csv <- ypc_gz %>% gsub(".gz", "", .)

yr_gz <- unified_gz %>%
  gsub(unified_dir, yr_dir, .) %>% 
  gsub("5-yr/hs-rev2007/hs-rev2007", "5-yr/yr", .)

yr_csv <- yr_gz %>% gsub(".gz", "", .)

yp_gz <- unified_gz %>%
  gsub(unified_dir, yp_dir, .) %>% 
  gsub("6-yp/hs-rev2007/hs-rev2007", "6-yp/yp", .)

yp_csv <- yp_gz %>% gsub(".gz", "", .)

yc_gz <- unified_gz %>%
  gsub(unified_dir, yc_dir, .) %>% 
  gsub("7-yc/hs-rev2007/hs-rev2007", "7-yc/yc", .)

yc_csv <- yc_gz %>% gsub(".gz", "", .)


# helpers -----------------------------------------------------------------

messageline <- function() {
  message(rep("-", 60))
}

extract <- function(x, y, z, t) {
  if (file.exists(y[[t]])) {
    messageline()
    message(paste(x[[t]], "already unzipped. Skipping."))
  } else {
    messageline()
    message(paste("Unzipping", x[[t]]))
    system(sprintf("7z e -aos %s -oc:%s", x[[t]], z))
  }
}

fread2 <- function(x, char = NULL, num = NULL) {
  messageline()
  message("function fread2")
  message("x: ", x)
  
  if (is.null(char) & is.null(num)) {
    d <- fread(
      paste("zcat", x)
    ) %>%
      as_tibble() %>% 
      clean_names()
  }
  
  if (!is.null(char) & is.null(num)) {
    d <- fread(
      paste("zcat", x),
      colClasses = list(
        character = char
      )
    ) %>%
      as_tibble() %>% 
      clean_names()
  }
  
  if (!is.null(char) & !is.null(num)) {
    d <- fread(
      paste("zcat", x),
      colClasses = list(
        character = char,
        numeric = num
      )
    ) %>%
      as_tibble() %>% 
      clean_names()
  }
  
  return(d)
}

file_remove <- function(x) {
  try(file.remove(x))
}

compress_gz <- function(x) {
  system(paste("gzip", x))
}

data_downloading <- function(t) {
  if (remove_old_files == 1 &
      (links$local_file_date[[t]] < links$server_file_date[[t]]) &
      !is.na(links$old_file[[t]])) {
    try(file.remove(links$old_file[[t]]))
  }
  if (!file.exists(links$new_file[[t]])) {
    message(paste("Downloading", links$new_file[[t]]))
    if (links$local_file_date[[t]] < links$server_file_date[[t]]) {
      Sys.sleep(sample(seq(5, 10, by = 1), 1))
      try(
        download.file(links$url[[t]],
                      links$new_file[[t]],
                      method = "wget",
                      quiet = T,
                      extra = "--no-check-certificate"
        )
      )
      
      if (file.size(links$new_file[[t]]) == 0) {
        fs <- 1
      } else {
        fs <- 0
      }
      
      while (fs > 0) {
        try(
          download.file(links$url[[t]],
                        links$new_file[[t]],
                        method = "wget",
                        quiet = T,
                        extra = "--no-check-certificate"
          )
        )
        
        if (file.size(links$new_file[[t]]) == 0) {
          fs <- fs + 1
        } else {
          fs <- 0
        }
      }
    } else {
      message(paste(
        "Existing data is not older than server data. Skipping",
        links$new_file[[t]]
      ))
    }
  } else {
    message(paste(links$new_file[[t]], "exists. Skiping."))
  }
}

compute_tidy_data <- function(t) {
  if (!file.exists(clean_gz[[t]])) {
    messageline()
    message(paste("Cleaning", years[[t]], "data..."))
    
    # clean data --------------------------------------------------------------
    
    clean_data <- fread(raw_csv[[t]], 
        colClasses = list(character = c("Commodity Code"), numeric = c("Trade Value (US$)"))
      ) %>%
      
      as_tibble() %>% 
      clean_names() %>%
      
      rename(trade_value_usd = trade_value_us) %>%
      select(trade_flow, reporter_iso, partner_iso, aggregate_level, commodity_code, trade_value_usd) %>%
      
      filter(aggregate_level %in% J) %>%
      filter(trade_flow %in% c("Export","Import")) %>%
      
      filter(
        !is.na(commodity_code),
        commodity_code != "",
        commodity_code != " "
      ) %>%
      
      mutate(
        reporter_iso = str_to_lower(reporter_iso),
        partner_iso = str_to_lower(partner_iso)
      ) %>%
      
      filter(
        reporter_iso %in% country_codes,
        partner_iso %in% country_codes
      )
    
    # exports data ------------------------------------------------------------
    
    exports <- clean_data %>%
      filter(trade_flow == "Export") %>%
      unite(pairs, reporter_iso, partner_iso, commodity_code, sep = "_") %>%
      select(pairs, trade_value_usd) %>% 
      mutate(trade_value_usd = ceiling(trade_value_usd))
    
    exports_mirrored <- clean_data %>%
      filter(trade_flow == "Import") %>%
      unite(pairs, partner_iso, reporter_iso, commodity_code, sep = "_") %>%
      select(pairs, trade_value_usd) %>% 
      mutate(trade_value_usd = ceiling(trade_value_usd / cif_fob_rate))
    
    rm(clean_data)
    
    exports_model <- exports %>% 
      full_join(exports_mirrored, by = "pairs") %>% 
      rowwise() %>% 
      mutate(trade_value_usd = max(trade_value_usd.x, trade_value_usd.y, na.rm = T)) %>% 
      ungroup() %>% 
      separate(pairs, c("reporter_iso", "partner_iso", "commodity_code"), sep = "_") %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)
    
    rm(exports, exports_mirrored)
    
    exports_model_low_granularity <- exports_model %>% 
      filter(str_length(commodity_code) == 4)
    
    exports_model_high_granularity <- exports_model %>% 
      filter(str_length(commodity_code) %in% c(5,6))
    
    rm(exports_model)
    
    exports_model_low_granularity_2 <- exports_model_high_granularity %>% 
      mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>% 
      group_by(reporter_iso, partner_iso, commodity_code) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T))
      
    exports_model_low_granularity <- exports_model_low_granularity %>% 
      left_join(exports_model_low_granularity_2, c("reporter_iso", "partner_iso", "commodity_code")) %>% 
      rowwise() %>% 
      mutate(trade_value_usd = max(trade_value_usd.x, trade_value_usd.y, na.rm = T)) %>% 
      ungroup() %>% 
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)
    
    rm(exports_model_low_granularity_2)
    
    exports_model <- exports_model_low_granularity %>% 
      bind_rows(exports_model_high_granularity) %>% 
      mutate(
        year = years[[t]],
        commodity_code_length = str_length(commodity_code)
      ) %>% 
      arrange(reporter_iso, partner_iso, commodity_code, commodity_code_length) %>% 
      select(year, reporter_iso, partner_iso, commodity_code, commodity_code_length, trade_value_usd)
    
    fwrite(exports_model, clean_csv[[t]])
    compress_gz(clean_csv[[t]])
  } else {
    messageline()
    message(paste("Skipping year", years[[t]], "Files exist."))
  }
}

convert_codes <- function(t, x, y, z) {
  equivalent_codes <- product_correspondence %>% 
    select(!!sym(c2[[dataset]]), hs07) %>% 
    filter(
      !(!!sym(c2[[dataset]]) %in% c("NULL")),
      !(hs07 %in% c("NULL"))
    ) %>% 
    mutate_if(is.character, str_sub, start = 1, end = 4) %>% 
    distinct(!!sym(c2[[dataset]]), hs07)
  
  if (!file.exists(z[[t]])) {
    data <- fread2(x[[t]], char = c("commodity_code"), num = c("trade_value_usd")) %>%
      left_join(equivalent_codes, by = c("commodity_code" = c2[[dataset]])) %>%
      distinct(reporter_iso, partner_iso, commodity_code, .keep_all = TRUE) %>% 
      mutate(
        hs07 = ifelse(is.na(hs07), 9999, hs07),
        commodity_code = hs07
      ) %>%
      select(-hs07) %>% 
      group_by(year, reporter_iso, partner_iso, commodity_code) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = TRUE))
    
    fwrite(data, y[[t]])
    compress_gz(y[[t]])
  }
}

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
    
    data <- fread2(d1, char = c("commodity_code"), num = c("trade_value_usd")) %>%
      unite(pairs, reporter_iso, partner_iso)
    
    if (!is.null(d2)) {
      data2 <- fread2(d2, char = c("commodity_code"), num = c("trade_value_usd")) %>%
        unite(pairs, reporter_iso, partner_iso) %>%
        anti_join(data, by = "pairs")
    } else {
      data2 <- NULL
    }
    
    if (!is.null(d3)) {
      data3 <- fread2(d3, char = c("commodity_code"), num = c("trade_value_usd")) %>%
        unite(pairs, reporter_iso, partner_iso) %>%
        anti_join(data, by = "pairs") %>%
        anti_join(data2, by = "pairs")
    }
    
    if (!is.null(d4)) {
      data4 <- fread2(d4, char = c("commodity_code"), num = c("trade_value_usd")) %>%
        unite(pairs, reporter_iso, partner_iso) %>%
        anti_join(data, by = "pairs") %>%
        anti_join(data2, by = "pairs") %>%
        anti_join(data3, by = "pairs")
    }
    
    if (!is.null(d5)) {
      data5 <- fread2(d5, char = c("commodity_code"), num = c("trade_value_usd")) %>%
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
    } else {
      data <- data %>%
        separate(pairs, c("reporter_iso", "partner_iso")) %>%
        arrange(reporter_iso, partner_iso, commodity_code)
    }
    
    fwrite(data, y[[t]])
    compress_gz(y[[t]])
  }
}

compute_rca <- function(x, y, z, keep, discard, t) {
  if (file.exists(z[[t]])) {
    messageline()
    message(paste0("Skipping year ", years[[t]], ". The file already exist."))
  } else {
    messageline()
    message(paste0(
      "Creating smooth RCA file for the year ", years[[t]], ". Be patient..."
    ))

    trade_t1 <- fread3(x[[t]]) %>%
      select(-!!sym(discard)) %>%
      unite(pairs, year, !!sym(keep), commodity_code, remove = TRUE) %>%
      group_by(pairs) %>%
      summarise(trade_value_usd_t1 = sum(trade_value_usd, na.rm = T)) %>%
      ungroup()

    if (years[[t]] <= years_missing_t_minus_1) {
      trade_t2 <- trade_t1 %>%
        select(pairs) %>%
        mutate(trade_value_usd_t2 = NA)
    } else {
      trade_t2 <- fread3(x[[t - 1]]) %>%
        select(-!!sym(discard)) %>%
        unite(pairs, year, !!sym(keep), commodity_code, remove = TRUE) %>%
        group_by(pairs) %>%
        summarise(trade_value_usd_t2 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
    }

    if (years[[t]] <= years_missing_t_minus_2) {
      trade_t3 <- trade_t1 %>%
        select(pairs) %>%
        mutate(trade_value_usd_t3 = NA)
    } else {
      trade_t3 <- fread3(x[[t - 2]]) %>%
        select(-!!sym(discard)) %>%
        unite(pairs, year, !!sym(keep), commodity_code, remove = FALSE) %>%
        group_by(pairs) %>%
        summarise(trade_value_usd_t3 = sum(trade_value_usd, na.rm = T)) %>%
        ungroup()
    }

    trade_t1 <- trade_t1 %>%
      left_join(trade_t2, by = "pairs") %>%
      left_join(trade_t3, by = "pairs") %>%
      rowwise() %>% # To apply a weighted mean by rows with 1 weight = 1 column
      mutate(
        xcp = weighted.mean( # x = value, c = country, p = product
          x = c(trade_value_usd_t1, trade_value_usd_t3, trade_value_usd_t3),
          w = c(2, 1, 1),
          na.rm = TRUE
        )
      ) %>%
      ungroup() %>%
      select(-c(trade_value_usd_t1, trade_value_usd_t2, trade_value_usd_t3)) %>%
      separate(pairs, c("year", keep, "commodity_code")) %>%
      group_by(commodity_code) %>% # Sum by product
      mutate(sum_p_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      group_by(!!sym(keep)) %>% # Sum by country
      mutate(sum_c_xcp = sum(xcp, na.rm = TRUE)) %>%
      ungroup() %>%
      mutate(
        sum_c_p_xcp = sum(xcp, na.rm = TRUE), # World's total exported value
        rca = (xcp / sum_c_xcp) / (sum_p_xcp / sum_c_p_xcp) # Compute RCA
      ) %>%
      select(year, !!sym(keep), commodity_code, rca)
    
    if (keep == "reporter_iso") {
      names(trade_t1) <- c("year", "country_iso", "commodity_code", "export_rca")
    } else {
      names(trade_t1) <- c("year", "country_iso", "commodity_code", "import_rca")
    }
    
    fwrite(trade_t1, y[[t]])
    compress_gz(y[[t]])
  }
}

compute_rca_exports <- function() {
  lapply(seq_along(years), compute_rca,
         x = unified_gz, y = rca_exports_csv, z = rca_exports_gz,
         keep = "reporter_iso", discard = "partner_iso"
  ) 
}

compute_rca_imports <- function() {
  lapply(seq_along(years), compute_rca,
         x = unified_gz, y = rca_imports_csv, z = rca_imports_gz,
         keep = "partner_iso", discard = "reporter_iso"
  )
}

compute_rca_metrics <- function(x, y, z, w, q, r, s, t) {
  if (!file.exists(gsub("csv", "csv.gz", s[[t]]))) {
    # RCA matrix --------------------------------------------------------------

    rca_matrix <- fread4(x[[t]]) %>%
      select(-year) %>%
      inner_join(select(ranking_1, reporter_iso), by = c("country_iso" = "reporter_iso")) %>%
      mutate(export_rca = ifelse(export_rca > 1, 1, 0)) %>%
      spread(commodity_code, export_rca)

    diversity <- rca_matrix %>% select(country_iso)
    ubiquity <- tibble(product = colnames(rca_matrix)) %>% filter(row_number() > 1)

    rca_matrix <- rca_matrix %>%
      select(-country_iso) %>%
      as.matrix()

    # convert to sparse class
    rca_matrix[is.na(rca_matrix)] <- 0
    rca_matrix <- Matrix(rca_matrix, sparse = T)

    diversity <- diversity %>%
      mutate(val = rowSums(rca_matrix, na.rm = TRUE)) %>%
      filter(val > 0)

    ubiquity <- ubiquity %>%
      mutate(val = colSums(rca_matrix, na.rm = TRUE)) %>%
      filter(val > 0)

    rownames(rca_matrix) <- diversity$country_iso

    D <- as.matrix(diversity$val, ncol = 1)
    U <- as.matrix(ubiquity$val, ncol = 1)

    # remove null rows and cols
    Mcp <- rca_matrix[
      which(rownames(rca_matrix) %in% unlist(diversity$country_iso)),
      which(colnames(rca_matrix) %in% unlist(ubiquity$product))
    ]
    rm(rca_matrix)

    # diversity and ubiquity following the Atlas notation
    kc0 <- as.numeric(D)
    kp0 <- as.numeric(U)

    # reflections method ------------------------------------------------------

    kcinv <- 1 / kc0
    kpinv <- 1 / kp0

    # create empty matrices
    kc <- Matrix(0, nrow = length(kc0), ncol = 20, sparse = T)
    kp <- Matrix(0, nrow = length(kp0), ncol = 20, sparse = T)

    # fill the first column with kc0 and kp0 to start iterating
    kc[, 1] <- kc0
    kp[, 1] <- kp0

    # compute cols 2 to 20 by iterating from col 1
    for (c in 2:ncol(kc)) {
      kc[, c] <- kcinv * (Mcp %*% kp[, (c - 1)])
      kp[, c] <- kpinv * (t(Mcp) %*% kc[, (c - 1)])
    }

    # ECI (reflections method) ------------------------------------------------

    eci_reflections <- as_tibble(
      (kc[, 19] - mean(kc[, 19])) / sd(kc[, 19])
    ) %>%
      mutate(country_iso = diversity$country_iso) %>%
      mutate(year = years[[t]]) %>%
      select(year, country_iso, value) %>%
      arrange(desc(value)) %>%
      rename(eci = value)

    fwrite(eci_reflections, y[[t]])
    compress_gz(y[[t]])

    # PCI (reflections method) ------------------------------------------------

    pci_reflections <- as_tibble(
      (kp[, 20] - mean(kp[, 20])) / sd(kp[, 20])
    ) %>%
      mutate(commodity_code = ubiquity$product) %>%
      mutate(year = years[[t]]) %>%
      select(year, commodity_code, value) %>%
      arrange(desc(value)) %>%
      rename(pci = value)

    fwrite(pci_reflections, z[[t]])
    compress_gz(z[[t]])

    rm(
      kc0,
      kp0,
      kcinv,
      kpinv,
      kc,
      kp,
      eci_reflections,
      pci_reflections,
      c
    )

    # proximity (countries) ---------------------------------------------------

    Phi_cc <- (Mcp %*% t(Mcp)) / proximity_countries_denominator(Mcp, D, cores = min(1,n_cores))

    Phi_cc_l <- Phi_cc
    Phi_cc_l[upper.tri(Phi_cc_l, diag = T)] <- NA

    Phi_cc_long <- as_tibble(as.matrix(Phi_cc_l)) %>%
      mutate(id = rownames(Phi_cc)) %>%
      gather(id2, value, -id) %>%
      filter(!is.na(value)) %>%
      setNames(c("country_iso", "country_iso_2", "value"))

    fwrite(Phi_cc_long, w[[t]])
    compress_gz(w[[t]])
    rm(Phi_cc_l, Phi_cc_long)

    # proximity (products) ---------------------------------------------------

    Phi_pp <- (t(Mcp) %*% Mcp) / proximity_products_denominator(Mcp, U, cores = min(1,n_cores))

    Phi_pp_l <- Phi_pp
    Phi_pp_l[upper.tri(Phi_pp_l, diag = T)] <- NA

    Phi_pp_long <- as_tibble(as.matrix(Phi_pp_l)) %>%
      mutate(id = rownames(Phi_pp)) %>%
      gather(id2, value, -id) %>%
      filter(!is.na(value)) %>%
      setNames(c("product_hs07_id", "product_hs07_id_2", "value"))

    fwrite(Phi_pp_long, q[[t]])
    compress_gz(q[[t]])
    rm(Phi_pp_l, Phi_pp_long)

    # density (countries) -----------------------------------------------------

    Omega_countries_cp <- (Phi_cc %*% Mcp) / colSums(Phi_cc)

    Omega_countries_cp_long <- as_tibble(as.matrix(Omega_countries_cp)) %>%
      mutate(country_iso = rownames(Omega_countries_cp)) %>%
      gather(product, value, -country_iso) %>%
      setNames(c("country_iso", "product_hs07_id", "value"))

    fwrite(Omega_countries_cp_long, r[[t]])
    compress_gz(r[[t]])
    rm(Omega_countries_cp, Omega_countries_cp_long)

    # density (products) ------------------------------------------------------

    Omega_products_cp <- t((Phi_pp %*% t(Mcp)) / colSums(Phi_pp))

    Omega_products_cp_long <- as_tibble(as.matrix(Omega_products_cp)) %>%
      mutate(country_iso = rownames(Omega_products_cp)) %>%
      gather(product, value, -country_iso) %>%
      setNames(c("country_iso", "product_hs07_id", "value"))

    fwrite(Omega_products_cp_long, s[[t]])
    compress_gz(s[[t]])
  }
}

summarise_trade <- function(d) {
  d %>% 
    summarise(
      export_value_usd = sum(export_value_usd, na.rm = T),
      import_value_usd = sum(import_value_usd, na.rm = T),
      
      export_value_usd_t2 = sum(export_value_usd_t2, na.rm = T),
      import_value_usd_t2 = sum(import_value_usd_t2, na.rm = T),
      
      export_value_usd_t3 = sum(export_value_usd_t3, na.rm = T),
      import_value_usd_t3 = sum(import_value_usd_t3, na.rm = T)
    )
}
  
compute_changes <- function(d) {
  d %>% 
    mutate(
      export_value_usd = ifelse(export_value_usd == 0, NA, export_value_usd),
      import_value_usd = ifelse(import_value_usd == 0, NA, import_value_usd),
      
      export_value_usd_t2 = ifelse(export_value_usd_t2 == 0, NA, export_value_usd_t2),
      import_value_usd_t2 = ifelse(import_value_usd_t2 == 0, NA, import_value_usd_t2),
      
      export_value_usd_t3 = ifelse(export_value_usd_t3 == 0, NA, export_value_usd_t3),
      import_value_usd_t3 = ifelse(import_value_usd_t3 == 0, NA, import_value_usd_t3)
    ) %>% 
    mutate(
      export_value_usd_change_1year = export_value_usd - export_value_usd_t2,
      export_value_usd_change_5years = export_value_usd - export_value_usd_t3,
      
      export_value_usd_percentage_change_1year = export_value_usd_change_1year / export_value_usd_t2,
      export_value_usd_percentage_change_5years = export_value_usd_change_5years / export_value_usd_t3,
      
      import_value_usd_change_1year = import_value_usd - import_value_usd_t2,
      import_value_usd_change_5years = import_value_usd - import_value_usd_t3,
      
      import_value_usd_percentage_change_1year = import_value_usd_change_1year / import_value_usd_t2,
      import_value_usd_percentage_change_5years = import_value_usd_change_5years / import_value_usd_t3
    )
}

compute_tables <- function(t) {
  if (file.exists(yrpc_gz[[t]])) {
    messageline()
    message(paste("yrpc table for the year", years[t], "exists. Skipping."))
  } else {
    messageline()
    message(paste("Creating yrpc table for the year", years[t]))
    
    # yrpc ------------------------------------------------------------------
    
    exports_t1 <- fread3(unified_gz[[t]]) %>% 
      rename(export_value_usd = trade_value_usd)
    
    imports_t1 <- exports_t1 %>% 
      select(matches("iso"), commodity_code, export_value_usd)
    
    names(imports_t1) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd")
    
    exports_t1 <- full_join(exports_t1, imports_t1) %>% 
      fill(year)
    
    yrpc_t1 <- exports_t1 %>%
      mutate(commodity_code_length = 4) %>%
      unite(pairs, reporter_iso, partner_iso, commodity_code) %>%
      select(year, pairs, commodity_code_length, export_value_usd, import_value_usd)
    
    rm(exports_t1, imports_t1)
    
    if (t %in% match(years_missing_t_minus_1, years)) {
      yrpc_t2 <- yrpc_t1 %>%
        select(pairs) %>%
        mutate(
          export_value_usd_t2 = NA,
          import_value_usd_t2 = NA
        )
    } else {
      exports_t2 <- fread3(unified_gz[[t - 1]]) %>% 
        rename(export_value_usd_t2 = trade_value_usd)
      
      imports_t2 <- exports_t2 %>% 
        select(matches("iso"), commodity_code, export_value_usd_t2)
      
      names(imports_t2) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t2")
      
      exports_t2 <- full_join(exports_t2, imports_t2)
      
      yrpc_t2 <- exports_t2 %>%
        unite(pairs, reporter_iso, partner_iso, commodity_code) %>%
        select(pairs, export_value_usd_t2, import_value_usd_t2)
      
      rm(exports_t2, imports_t2)
    }
    
    if (t %in% match(years_missing_t_minus_5, years) | t %in% match(years_missing_t_minus_1, years)) {
      yrpc_t3 <- yrpc_t1 %>%
        select(pairs) %>%
        mutate(
          export_value_usd_t3 = NA,
          import_value_usd_t3 = NA
        )
    } else {
      exports_t3 <- fread3(unified_gz[[t - 5]]) %>% 
        rename(export_value_usd_t3 = trade_value_usd)
      
      imports_t3 <- exports_t3 %>% 
        select(matches("iso"), commodity_code, export_value_usd_t3)
      
      names(imports_t3) <- c("partner_iso", "reporter_iso", "commodity_code", "import_value_usd_t3")
      
      exports_t3 <- full_join(exports_t3, imports_t3)
      
      yrpc_t3 <- exports_t3 %>%
        unite(pairs, reporter_iso, partner_iso, commodity_code) %>%
        select(pairs, export_value_usd_t3, import_value_usd_t3)
      
      rm(exports_t3, imports_t3)
    }
    
    yrpc_t1 <- yrpc_t1 %>%
      mutate(
        export_value_usd = ifelse(export_value_usd == 0, NA, export_value_usd),
        import_value_usd = ifelse(import_value_usd == 0, NA, import_value_usd)
      )
    
    if (class(yrpc_t2$export_value_usd_t2) != "logical") {
      yrpc_t2 <- yrpc_t2 %>%
        mutate(
          export_value_usd_t2 = ifelse(export_value_usd_t2 == 0, NA, export_value_usd_t2),
          import_value_usd_t2 = ifelse(import_value_usd_t2 == 0, NA, import_value_usd_t2)
        )
    }
    
    if (class(yrpc_t3$export_value_usd_t3) != "logical") {
      yrpc_t3 <- yrpc_t3 %>%
        mutate(
          export_value_usd_t3 = ifelse(export_value_usd_t3 == 0, NA, export_value_usd_t3),
          import_value_usd_t3 = ifelse(import_value_usd_t3 == 0, NA, import_value_usd_t3)
        )
    }
    
    yrpc <- yrpc_t1 %>%
      left_join(yrpc_t2) %>%
      left_join(yrpc_t3) %>%
      compute_changes() %>%
      separate(pairs, c("reporter_iso", "partner_iso", "commodity_code"))
    
    if (class(yrpc$export_value_usd_t2) != "numeric") {
      yrpc <- yrpc %>%
        mutate(
          export_value_usd_t2 = as.double(export_value_usd_t2),
          export_value_usd_t3 = as.double(export_value_usd_t3),
          import_value_usd_t2 = as.double(import_value_usd_t2),
          import_value_usd_t3 = as.double(import_value_usd_t3)
        )
    }
    
    rm(yrpc_t1, yrpc_t2, yrpc_t3)
    
    fwrite(
      yrpc %>%
        select(-c(ends_with("_t2"), ends_with("_t3"))),
      yrpc_csv[[t]]
    )
    
    compress_gz(yrpc_csv[[t]])
    
    # yrp ------------------------------------------------------------------
    
    yrp <- yrpc %>%
      select(year, reporter_iso, partner_iso, matches("export"), matches("import")) %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrp, yrp_csv[[t]])
    compress_gz(yrp_csv[[t]])
    rm(yrp)
    
    # yrc ------------------------------------------------------------------
    
    rca_exp <- fread4(rca_exports_gz[[t]]) %>%
      select(-year)
    
    rca_imp <- fread4(rca_imports_gz[[t]]) %>%
      select(-year)
    
    yrc <- yrpc %>%
      select(year, reporter_iso, commodity_code, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, reporter_iso, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>%
      left_join(rca_exp, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      left_join(rca_imp, by = c("reporter_iso" = "country_iso", "commodity_code")) %>%
      compute_changes() %>% 
      select(year, reporter_iso, commodity_code, everything()) %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yrc, yrc_csv[[t]])
    compress_gz(yrc_csv[[t]])
    rm(yrc, rca_exp, rca_imp)
    
    # ypc ------------------------------------------------------------------
    
    ypc <- yrpc %>%
      select(year, partner_iso, commodity_code, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, partner_iso, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>%
      compute_changes() %>%
      select(year, partner_iso, commodity_code, everything()) %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(ypc, ypc_csv[[t]])
    compress_gz(ypc_csv[[t]])
    rm(ypc)
    
    # yr -------------------------------------------------------------------
    
    max_exp <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(export_value_usd)) %>%
      rename(
        top_export_commodity_code = commodity_code,
        top_export_value_usd = export_value_usd
      )
    
    max_imp <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(reporter_iso) %>%
      slice(which.max(import_value_usd)) %>%
      rename(
        top_import_commodity_code = commodity_code,
        top_import_value_usd = import_value_usd
      )
    
    yr <- yrpc %>%
      select(year, reporter_iso, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, reporter_iso) %>%
      summarise_trade() %>%
      ungroup() %>% 
      left_join(max_exp, by = "reporter_iso") %>%
      left_join(max_imp, by = "reporter_iso") %>%
      compute_changes() %>% 
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yr, yr_csv[[t]])
    compress_gz(yr_csv[[t]])
    rm(yr, max_exp, max_imp)
    
    # yp -------------------------------------------------------------------
    
    yp <- yrpc %>%
      select(year, partner_iso, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, partner_iso) %>%
      summarise_trade() %>%
      ungroup() %>%
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3")))
    
    fwrite(yp, yp_csv[[t]])
    compress_gz(yp_csv[[t]])
    rm(yp)
    
    # yc -------------------------------------------------------------------
    
    pci_t1 <- pci %>%
      filter(year == years[[t]]) %>%
      select(-year)
    
    if (t %in% match(years_missing_t_minus_1, years)) {
      pci_t2 <- pci_t1 %>%
        mutate(
          pci_rank = NA,
          pci = NA
        )
    } else {
      pci_t2 <- pci %>%
        filter(year == years[[t - 1]]) %>%
        select(-year)
    }
    
    max_exp_2 <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(export_value_usd = sum(export_value_usd, na.rm = T)) %>%
      group_by(commodity_code) %>%
      slice(which.max(export_value_usd)) %>%
      rename(top_exporter_iso = reporter_iso) %>%
      select(-export_value_usd)
    
    max_imp_2 <- yrpc %>%
      group_by(reporter_iso, commodity_code) %>%
      summarise(import_value_usd = sum(import_value_usd, na.rm = T)) %>%
      group_by(commodity_code) %>%
      slice(which.max(import_value_usd)) %>%
      rename(top_importer_iso = reporter_iso) %>%
      select(-import_value_usd)
    
    yc <- yrpc %>%
      select(year, commodity_code, matches("export_value_usd"), matches("import_value_usd")) %>%
      group_by(year, commodity_code) %>%
      summarise_trade() %>%
      ungroup() %>% 
      left_join(pci_t1, by = "commodity_code") %>%
      left_join(pci_t2, by = "commodity_code") %>%
      mutate(pci_rank_delta = pci_rank.x - pci_rank.y) %>%
      select(-c(pci_rank.y, pci.y)) %>%
      rename(
        pci = pci.x,
        pci_rank = pci_rank.x
      ) %>%
      left_join(max_exp_2, by = "commodity_code") %>%
      left_join(max_imp_2, by = "commodity_code") %>%
      compute_changes() %>%
      select(-c(ends_with("_t2"), ends_with("_t3"))) %>% 
      select(year, commodity_code, everything())
    
    fwrite(yc, yc_csv[[t]])
    compress_gz(yc_csv[[t]])
    rm(yrpc, yc, pci_t1, pci_t2, max_exp_2, max_imp_2)
  }
}

