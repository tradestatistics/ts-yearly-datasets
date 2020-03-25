# Open ts-yearly-data.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

copy_to_db <- function(overwrite = F) {
  # messages ----------------------------------------------------------------
  
  message("Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
          This file is part of Open Trade Statistics project.
          The scripts within this project are released under GNU General Public License 3.0.\n
          This program is free software and comes with ABSOLUTELY NO WARRANTY.
          You are welcome to redistribute it under certain conditions.
          See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.\n")
  
  readline(prompt = "Press [enter] to continue if and only if you agree to the license terms")
  
  # scripts -----------------------------------------------------------------
  
  ask_for_db_access <<- 1
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")
  
  # attributes --------------------------------------------------------------
  
  message("Copying attributes...")
  
  # countries attributes
  
  obs_attributes_countries <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_countries"))
  
  if (obs_attributes_countries == 0) {
    attributes_countries <- readRDS(paste0(tables_dir, "/0-attributes/attributes_countries.rds"))
    dbWriteTable(con, "attributes_countries", attributes_countries, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # products attributes
  
  obs_attributes_products <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_products"))
  
  if (obs_attributes_products == 0) {
    attributes_products <- readRDS(paste0(tables_dir, "/0-attributes/attributes_products.rds")) %>% 
      filter(str_length(product_code) == 4)
    dbWriteTable(con, "attributes_products", attributes_products, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  obs_attributes_products_shortnames <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_products_shortnames"))
  
  if (obs_attributes_products_shortnames == 0) {
    attributes_products_shortnames <- readRDS(paste0(tables_dir, "/0-attributes/attributes_products_shortnames.rds"))
    dbWriteTable(con, "attributes_products_shortnames", attributes_products_shortnames, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # groups attributes
  
  obs_attributes_groups <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_groups"))
  
  if (obs_attributes_groups == 0) {
    attributes_groups <- readRDS(paste0(tables_dir, "/0-attributes/attributes_products.rds")) %>% 
      select(group_code, group_fullname_english) %>% 
      distinct()
    dbWriteTable(con, "attributes_groups", attributes_groups, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # sections attributes
  
  obs_attributes_sections <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_sections"))
  
  if (obs_attributes_sections == 0) {
    attributes_sections <- readRDS(paste0(tables_dir, "/0-attributes/attributes_sections.rds")) %>% 
      filter(str_length(product_code) == 4) %>% 
      select(product_code, section_code)
    dbWriteTable(con, "attributes_sections", attributes_sections, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  obs_attributes_sections_names <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_sections_names"))
  
  if (obs_attributes_sections_names == 0) {
    attributes_sections_names <- readRDS(paste0(tables_dir, "/0-attributes/attributes_sections.rds")) %>% 
      select(section_code, section_fullname_english) %>% 
      distinct()
    
    dbWriteTable(con, "attributes_sections_names", attributes_sections_names, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  obs_attributes_sections_shortnames <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_sections_shortnames"))
  
  if (obs_attributes_sections_shortnames == 0) {
    attributes_sections_shortnames <- readRDS(paste0(tables_dir, "/0-attributes/attributes_sections.rds")) %>% 
      select(section_code, section_shortname_english) %>% 
      distinct()
    
    dbWriteTable(con, "attributes_sections_shortnames", attributes_sections_shortnames, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  obs_attributes_sections_colors <- as.numeric(dbGetQuery(con, "SELECT COUNT(*) FROM public.attributes_sections_colors"))
  
  if (obs_attributes_sections_colors == 0) {
    attributes_sections_colors <- readRDS(paste0(tables_dir, "/0-attributes/attributes_sections.rds")) %>% 
      select(section_code, section_color) %>% 
      distinct()
    
    dbWriteTable(con, "attributes_sections_colors", attributes_sections_colors, append = TRUE, overwrite = overwrite, row.names = FALSE)
  }
  
  # data --------------------------------------------------------------------
  
  lapply(
    rev(seq_along(years_full)),
    function(t) {
      obs_yrpc <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yrpc WHERE year = %s", years_full[t])))
      
      if (obs_yrpc == 0) {
        message(sprintf("Copying year %s to table YRPC...", years_full[t]))
        yrpc <- readRDS(yrpc_rds[t])
        dbWriteTable(con, "hs07_yrpc", yrpc, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yrpc)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YRPC.", years_full[t]))
      }
    }
  )
  
  lapply(
    rev(seq_along(years_full)),
    function(t) {
      obs_yrp <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yrp WHERE year = %s", years_full[t])))
      
      if (obs_yrp == 0) {
        message(sprintf("Copying year %s to table YRP...", years_full[t]))
        
        yrp <- readRDS(yrp_rds[t])
        dbWriteTable(con, "hs07_yrp", yrp, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yrp)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YRP.", years_full[t]))
      }
    }
  )
  
  lapply(
    rev(seq_along(years_full)),
    function(t) {
      obs_yrc <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yrc WHERE year = %s", years_full[t])))
      
      if (obs_yrc == 0) {
        message(sprintf("Copying year %s to table YRP...", years_full[t]))
        
        yrc <- readRDS(yrc_rds[t])
        dbWriteTable(con, "hs07_yrc", yrc, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yrc)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YRC.", years_full[t]))
      }
    }
  )
  
  lapply(
    rev(seq_along(years_full)),
    function(t) {
      obs_yr <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yr WHERE year = %s", years_full[t])))
      
      if (obs_yr == 0) {
        message(sprintf("Copying year %s to table YR...", years_full[t]))
        
        yr <- readRDS(yr_rds[t])
        dbWriteTable(con, "hs07_yr", yr, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yr)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YR.", years_full[t]))
      }
    }
  )
  
  lapply(
    rev(seq_along(years_full)),
    function(t) {
      obs_yc <- as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(year) FROM public.hs07_yc WHERE year = %s", years_full[t])))
      
      if (obs_yc == 0) {
        message(sprintf("Copying year %s to table YC...", years_full[t]))
        
        yc <- readRDS(yc_rds[t])
        dbWriteTable(con, "hs07_yc", yc, append = TRUE, overwrite = overwrite, row.names = FALSE)
        rm(yc)
      } else {
        message(sprintf("Skipping year %s. Already exists in table YC.", years_full[t]))
      }
    }
  )
}

copy_to_db()
