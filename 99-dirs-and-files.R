# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

# dirs/files --------------------------------------------------------

# 0-1-download-data.R

raw_dir <- "01-raw-data"
try(dir.create(raw_dir))

raw_dir <- sprintf("%s/%s-rev%s", raw_dir, classification, revision)
try(dir.create(raw_dir))

raw_dir_zip <- sprintf("%s/%s", raw_dir, "zip")
try(dir.create(raw_dir_zip))

raw_dir_rds <- str_replace(raw_dir_zip, "zip", "rds")
try(dir.create(raw_dir_rds))

# 0-2-clean-data.R

raw_zip <- list.files(
  path = raw_dir_zip,
  pattern = "\\.zip",
  full.names = T
) %>%
  grep(paste(paste0("ps-", years), collapse = "|"), ., value = TRUE)

raw_rds <- list.files(
  path = raw_dir_rds,
  pattern = "\\.rds",
  full.names = T
)

clean_dir <- "02-clean-data"
rev_dir <- sprintf("%s/%s-rev%s", clean_dir, classification, revision)
try(dir.create(clean_dir))
try(dir.create(rev_dir))

clean_rds <- sprintf("02-clean-data/%s-rev%s/%s-rev%s-%s.rds", classification, revision, classification, revision, years)

# 0-3-convert-data.R

c1 <- c("hs-rev1992", "hs-rev1996", "hs-rev2002", "hs-rev2007", "sitc-rev1", "sitc-rev2")

c2 <- c("hs92", "hs96", "hs02", "hs07", "sitc1", "sitc2")

converted_dir <- "03-converted-data"
try(dir.create(converted_dir))

try(dir.create(paste(converted_dir, c1[dataset], sep = "/")))

converted_rds <- grep(c1[dataset], clean_rds, value = TRUE) %>%
  str_replace(clean_dir, converted_dir)

# 0-4-unify-data.R

unified_dir <- "04-unified-data"
try(dir.create(unified_dir))
try(dir.create(paste0(unified_dir, "/", c1[dataset])))

clean_rds_all <- list.files(clean_dir, recursive = T, full.names = T)

converted_rds_all <- list.files(converted_dir, recursive = T, full.names = T)

unified_rds <- paste0(unified_dir, "/", c1[dataset], "/", c1[dataset], "-", years_full, ".rds")

# 0-5-compute-metrics.R

metrics_dir <- "05-metrics"
try(dir.create(metrics_dir))

rca_exports_dir <- sprintf("%s/hs-rev2007-rca-exports", metrics_dir)
try(dir.create(rca_exports_dir))
rca_exports_rds <- sprintf("%s/rca-exports-%s.rds", rca_exports_dir, years_full)

rca_imports_dir <- sprintf("%s/hs-rev2007-rca-imports", metrics_dir)
try(dir.create(rca_imports_dir))
rca_imports_rds <- sprintf("%s/rca-imports-%s.rds", rca_imports_dir, years_full)

proximity_countries_dir <- sprintf("%s/hs-rev2007-proximity-countries", metrics_dir)
try(dir.create(proximity_countries_dir))
proximity_countries_rds <- sprintf("%s/proximity-countries-%s.rds", proximity_countries_dir, years_full)

proximity_products_dir <- sprintf("%s/hs-rev2007-proximity-products", metrics_dir)
try(dir.create(proximity_products_dir))
proximity_products_rds <- sprintf("%s/proximity-products-%s.rds", proximity_products_dir, years_full)

eci_dir <- sprintf("%s/hs-rev2007-eci", metrics_dir)
try(dir.create(eci_dir))

eci_rankings_r_rds <- sprintf("%s/eci-reflections-%s.rds", eci_dir, years_full)
eci_rankings_e_rds <- sprintf("%s/eci-eigenvalues-%s.rds", eci_dir, years_full)
eci_rankings_f_rds <- sprintf("%s/eci-fitness-%s.rds", eci_dir, years_full)

pci_dir <- sprintf("%s/hs-rev2007-pci", metrics_dir)
try(dir.create(pci_dir))

pci_rankings_r_rds <- sprintf("%s/pci-reflections-%s.rds", pci_dir, years_full)
pci_rankings_e_rds <- sprintf("%s/pci-eigenvalues-%s.rds", pci_dir, years_full)
pci_rankings_f_rds <- sprintf("%s/pci-fitness-%s.rds", pci_dir, years_full)

eci_files <- paste0(eci_dir, c("/eci-reflections-joined-ranking.rds",
               "/eci-eigenvalues-joined-ranking.rds",
               "/eci-fitness-joined-ranking.rds"))

pci_files <- paste0(pci_dir, c("/pci-reflections-joined-ranking.rds",
               "/pci-eigenvalues-joined-ranking.rds",
               "/pci-fitness-joined-ranking.rds"))

# 0-6-create-tables.R

tables_dir <- "06-tables"
try(dir.create(tables_dir))

tables_dir <- sprintf("%s/hs-rev2007", tables_dir)
try(dir.create(tables_dir))

attributes_dir <- paste0(tables_dir, "/0-attributes")
try(dir.create(attributes_dir))

yrpc_dir <- paste0(tables_dir, "/1-yrpc")
try(dir.create(yrpc_dir))

yrp_dir <- paste0(tables_dir, "/2-yrp")
try(dir.create(yrp_dir))

yrc_dir <- paste0(tables_dir, "/3-yrc")
try(dir.create(yrc_dir))

yr_dir <- paste0(tables_dir, "/4-yr")
try(dir.create(yr_dir))

yc_dir <- paste0(tables_dir, "/5-yc")
try(dir.create(yc_dir))

yrpc_rds <- unified_rds %>%
  gsub(unified_dir, yrpc_dir, .) %>%
  gsub(paste0("/1-yrpc/", classification, "-rev", revision), "/1-yrpc", .) %>% 
  gsub(paste0("/1-yrpc/", classification, "-rev", revision), "/1-yrpc/yrpc", .)

yrp_rds <- unified_rds %>%
  gsub(unified_dir, yrp_dir, .) %>%
  gsub(paste0("/2-yrp/", classification, "-rev", revision), "/2-yrp", .) %>% 
  gsub(paste0("/2-yrp/", classification, "-rev", revision), "/2-yrp/yrp", .)

yrc_rds <- unified_rds %>%
  gsub(unified_dir, yrc_dir, .) %>%
  gsub(paste0("/3-yrc/", classification, "-rev", revision), "/3-yrc", .) %>% 
  gsub(paste0("/3-yrc/", classification, "-rev", revision), "/3-yrc/yrc", .)

yr_rds <- unified_rds %>%
  gsub(unified_dir, yr_dir, .) %>%
  gsub(paste0("/4-yr/", classification, "-rev", revision), "/4-yr", .) %>% 
  gsub(paste0("/4-yr/", classification, "-rev", revision), "/4-yr/yr", .)

yc_rds <- unified_rds %>%
  gsub(unified_dir, yc_dir, .) %>%
  gsub(paste0("/5-yc/", classification, "-rev", revision), "/5-yc", .) %>% 
  gsub(paste0("/5-yc/", classification, "-rev", revision), "/5-yc/yc", .)
