# Open ts-yearly-data.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

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

clean_dir <- "02-clean-data"
rev_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, revision)
try(dir.create(clean_dir))
try(dir.create(rev_dir))

clean_gz <- sprintf("02-clean-data/%s-rev%s/%s-rev%s-%s.csv.gz", classification, revision, classification, revision, years)

# 0-3-convert-data.R

c1 <- c("hs-rev1992", "hs-rev1996", "hs-rev2002", "hs-rev2007", "sitc-rev1", "sitc-rev2")

c2 <- c("hs92", "hs96", "hs02", "hs07", "sitc1", "sitc2")

converted_dir <- "03-converted-data"
try(dir.create(converted_dir))

try(dir.create(paste(converted_dir, c1[dataset], sep = "/")))

converted_gz <- grep(c1[dataset], clean_gz, value = TRUE) %>% 
  str_replace(clean_dir, converted_dir)

# 0-4-unify-data.R

unified_dir <- "04-unified-data"
try(dir.create(unified_dir))
try(dir.create(paste0(unified_dir, "/", c1[dataset])))

clean_gz_all <- list.files(clean_dir, recursive = T, full.names = T)

converted_gz_all <- list.files(converted_dir, recursive = T, full.names = T)

unified_gz <- paste0(unified_dir, "/", c1[dataset], "/", c1[dataset], "-", years_all_classifications, ".csv.gz")

# 0-5-compute-metrics.R

metrics_dir <- "04-metrics"
try(dir.create(metrics_dir))

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

yrpc_gz <- converted_gz %>%
  gsub(converted_dir, yrpc_dir, .) %>% 
  gsub("1-yrpc/hs-rev2007/hs-rev2007", "1-yrpc/yrpc", .)

yrpc_csv <- yrpc_gz %>% gsub(".gz", "", .)

yrp_gz <- converted_gz %>%
  gsub(converted_dir, yrp_dir, .) %>% 
  gsub("2-yrp/hs-rev2007/hs-rev2007", "2-yrp/yrp", .)

yrp_csv <- yrp_gz %>% gsub(".gz", "", .)

yrc_gz <- converted_gz %>%
  gsub(converted_dir, yrc_dir, .) %>% 
  gsub("3-yrc/hs-rev2007/hs-rev2007", "3-yrc/yrc", .)

yrc_csv <- yrc_gz %>% gsub(".gz", "", .)

ypc_gz <- converted_gz %>%
  gsub(converted_dir, ypc_dir, .) %>% 
  gsub("4-ypc/hs-rev2007/hs-rev2007", "4-ypc/ypc", .)

ypc_csv <- ypc_gz %>% gsub(".gz", "", .)

yr_gz <- converted_gz %>%
  gsub(converted_dir, yr_dir, .) %>% 
  gsub("5-yr/hs-rev2007/hs-rev2007", "5-yr/yr", .)

yr_csv <- yr_gz %>% gsub(".gz", "", .)

yp_gz <- converted_gz %>%
  gsub(converted_dir, yp_dir, .) %>% 
  gsub("6-yp/hs-rev2007/hs-rev2007", "6-yp/yp", .)

yp_csv <- yp_gz %>% gsub(".gz", "", .)

yc_gz <- converted_gz %>%
  gsub(converted_dir, yc_dir, .) %>% 
  gsub("7-yc/hs-rev2007/hs-rev2007", "7-yc/yc", .)

yc_csv <- yc_gz %>% gsub(".gz", "", .)
