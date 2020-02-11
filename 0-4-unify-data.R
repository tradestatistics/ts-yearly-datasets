# Open ts-yearly-datasets.Rproj before running this function

# Copyright (C) 2018-2019, Mauricio \"Pacha\" Vargas.
# This file is part of Open Trade Statistics project.
# The scripts within this project are released under GNU General Public License 3.0.
# This program is free software and comes with ABSOLUTELY NO WARRANTY.
# You are welcome to redistribute it under certain conditions.
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details.

unify <- function() {
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
  
  source("99-user-input.R")
  source("99-input-based-parameters.R")
  source("99-packages.R")
  source("99-funs.R")
  source("99-dirs-and-files.R")

  # functions ---------------------------------------------------------------

  join_datasets <- function(x, y, z, t) {
    converted_codes <- c1[dataset]
    
    if (!file.exists(z[t])) {
      if (years_full[t] < 1976) {
        f1 <- NULL
        f2 <- NULL
        f3 <- NULL
        f4 <- NULL
        f5 <- ifelse(converted_codes == c1[5],
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), y, value = T)
        )
        f6 <- NULL
      }
      
      if (years_full[t] >= 1976 & years_full[t] < 1992) {
        f1 <- NULL
        f2 <- NULL
        f3 <- NULL
        f4 <- NULL
        f5 <- ifelse(converted_codes == c1[5],
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), y, value = T)
        )
        f6 <- ifelse(converted_codes == c1[6],
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), y, value = T)
        )
      }
      
      if (years_full[t] >= 1992 & years_full[t] < 1996) {
        f1 <- ifelse(converted_codes == c1[1],
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), y, value = T)
        )
        f2 <- NULL
        f3 <- NULL
        f4 <- NULL
        f5 <- ifelse(converted_codes == c1[5],
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), y, value = T)
        )
        f6 <- ifelse(converted_codes == c1[6],
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), y, value = T)
        )
      }
      
      if (years_full[t] >= 1996 & years_full[t] < 2002) {
        f1 <- ifelse(converted_codes == c1[1],
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), y, value = T)
        )
        f2 <- ifelse(converted_codes == c1[2],
                     grep(paste0(c1[2], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[2], "-", years_full[t], ".rds"), y, value = T)
        )
        f3 <- NULL
        f4 <- NULL
        f5 <- ifelse(converted_codes == c1[5],
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), y, value = T)
        )
        f6 <- ifelse(converted_codes == c1[6],
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), y, value = T)
        )
      }
      
      if (years_full[t] >= 2002 & years_full[t] < 2007) {
        f1 <- ifelse(converted_codes == c1[1],
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), y, value = T)
        )
        f2 <- ifelse(converted_codes == c1[2],
                     grep(paste0(c1[2], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[2], "-", years_full[t], ".rds"), y, value = T)
        )
        f3 <- ifelse(converted_codes == c1[3],
                     grep(paste0(c1[3], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[3], "-", years_full[t], ".rds"), y, value = T)
        )
        f4 <- NULL
        f5 <- ifelse(converted_codes == c1[5],
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), y, value = T)
        )
        f6 <- ifelse(converted_codes == c1[6],
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), y, value = T)
        )
      }
      
      if (years_full[t] >= 2007) {
        f1 <- ifelse(converted_codes == c1[1],
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[1], "-", years_full[t], ".rds"), y, value = T)
        )
        f2 <- ifelse(converted_codes == c1[2],
                     grep(paste0(c1[2], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[2], "-", years_full[t], ".rds"), y, value = T)
        )
        f3 <- ifelse(converted_codes == c1[3],
                     grep(paste0(c1[3], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[3], "-", years_full[t], ".rds"), y, value = T)
        )
        f4 <- ifelse(converted_codes == c1[4],
                     grep(paste0(c1[4], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[4], "-", years_full[t], ".rds"), y, value = T)
        )
        f5 <- ifelse(converted_codes == c1[5],
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[5], "-", years_full[t], ".rds"), y, value = T)
        )
        f6 <- ifelse(converted_codes == c1[6],
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), x, value = T),
                     grep(paste0(c1[6], "-", years_full[t], ".rds"), y, value = T)
        )
      }
      
      files <- c(f4, f3, f2, f1, f6, f5)
      
      leading_file <- files[1]
      
      if (length(leading_file) == 0) {
        leading_file <- files[length(files) != 0][1]
      }
      
      complementary_files <- files[files != leading_file]
      
      if (length(complementary_files) == 0) {
        complementary_files <- rep(0, 5)
      }
      
      if (length(complementary_files) != 5) {
        complementary_files <- c(complementary_files, rep(0, 5 - length(complementary_files)))
      }
      
      messageline()
      message(paste("Reading", leading_file))
      
      data <- readRDS(leading_file) %>% 
        filter(product_code_length == 4)
      
      if (complementary_files[1] != 0) {
        messageline()
        message(paste("Reading", complementary_files[1]))
        
        data2 <- readRDS(complementary_files[1]) %>%
          filter(product_code_length == 4) %>% 
          anti_join(data, by = c("reporter_iso", "partner_iso"))
      } else {
        data2 <- NULL
      }
      
      if (complementary_files[2] != 0) {
        messageline()
        message(paste("Reading", complementary_files[2]))
        
        data3 <- readRDS(complementary_files[2]) %>%
          filter(product_code_length == 4) %>% 
          anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data2, by = c("reporter_iso", "partner_iso"))
      } else {
        data3 <- NULL
      }
      
      if (complementary_files[3] != 0) {
        messageline()
        message(paste("Reading", complementary_files[3]))
        
        data4 <- readRDS(complementary_files[3]) %>%
          filter(product_code_length == 4) %>% 
          anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data3, by = c("reporter_iso", "partner_iso"))
      } else {
        data4 <- NULL
      }
      
      if (complementary_files[4] != 0) {
        messageline()
        message(paste("Reading", complementary_files[4]))
        
        data5 <- readRDS(complementary_files[4]) %>%
          filter(product_code_length == 4) %>% 
          anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data3, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data4, by = c("reporter_iso", "partner_iso"))
      } else {
        data5 <- NULL
      }
      
      if (complementary_files[5] != 0) {
        messageline()
        message(paste("Reading", complementary_files[5]))
        
        data6 <- readRDS(complementary_files[5]) %>%
          filter(product_code_length == 4) %>% 
          anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data3, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data4, by = c("reporter_iso", "partner_iso")) %>%
          anti_join(data5, by = c("reporter_iso", "partner_iso"))
      } else {
        data6 <- NULL
      }
      
      data <- bind_rows(data, data2, data3, data4, data5, data6) %>%
        arrange(reporter_iso, partner_iso, product_code)
      
      saveRDS(data, file = z[t], compress = "xz")
    }
  }
  
  # convert data ------------------------------------------------------------

  if (operating_system != "Windows") {
    mclapply(seq_along(years_full), join_datasets,
      mc.cores = n_cores,
      x = clean_rds_all, y = converted_rds_all, z = unified_rds
    )
  } else {
    lapply(seq_along(years_full), join_datasets,
      x = clean_rds_all, y = converted_rds_all, z = unified_rds
    )
  }
}

unify()
