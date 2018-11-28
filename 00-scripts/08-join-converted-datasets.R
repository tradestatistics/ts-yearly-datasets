# Open ts-yearly-datasets.Rproj before running this function

# Copyright (c) 2018, Mauricio \"Pacha\" Vargas
# This file is part of Open Trade Statistics project
# The scripts within this project are released under GNU General Public License 3.0
# See https://github.com/tradestatistics/ts-yearly-datasets/LICENSE for the details

join_datasets <- function(x, y, z, t) {
  converted_codes <- c1[dataset]
  
  if (!file.exists(z[t])) {
    if (years_full[t] < 1976) {
      f1 <- NULL
      f2 <- NULL
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f6 <- NULL
    }
    
    if (years_full[t] >= 1976 & years_full[t] < 1992) {
      f1 <- NULL
      f2 <- NULL
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), y, value = T)
      )
    }
    
    if (years_full[t] >= 1992 & years_full[t] < 1996) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f2 <- NULL
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), y, value = T)
      )
    }
    
    if (years_full[t] >= 1996 & years_full[t] < 2002) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f2 <- ifelse(converted_codes == c1[2], 
                   grep(paste0(c1[2], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[2], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), y, value = T)
      )
    }
    
    if (years_full[t] >= 2002 & years_full[t] < 2007) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f2 <- ifelse(converted_codes == c1[2], 
                   grep(paste0(c1[2], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[2], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f3 <- ifelse(converted_codes == c1[3], 
                   grep(paste0(c1[3], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[3], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), y, value = T)
      )
    }
    
    if (years_full[t] >= 2007) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f2 <- ifelse(converted_codes == c1[2], 
                   grep(paste0(c1[2], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[2], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f3 <- ifelse(converted_codes == c1[3], 
                   grep(paste0(c1[3], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[3], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f4 <- ifelse(converted_codes == c1[4], 
                   grep(paste0(c1[4], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[4], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_full[t], ".csv.gz"), y, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_full[t], ".csv.gz"), y, value = T)
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
      complementary_files = c(complementary_files, rep(0, 5 - length(complementary_files)))
    }
    
    data <- fread2(leading_file, character = "commodity_code",  numeric = "trade_value_usd")
    
    if (complementary_files[1] != 0) {
      data2 <- fread2(complementary_files[1], character = "commodity_code",  numeric = "trade_value_usd") %>%
        anti_join(data, by = c("reporter_iso", "partner_iso"))
    } else {
      data2 <- NULL
    }
    
    if (complementary_files[2] != 0) {
      data3 <- fread2(complementary_files[2], character = "commodity_code",  numeric = "trade_value_usd") %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso"))
    } else {
      data3 <- NULL
    }
    
    if (complementary_files[3] != 0) {
      data4 <- fread2(complementary_files[3], character = "commodity_code",  numeric = "trade_value_usd") %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data3, by = c("reporter_iso", "partner_iso"))
    } else {
      data4 <- NULL
    }
    
    if (complementary_files[4] != 0) {
      data5 <- fread2(complementary_files[4], character = "commodity_code",  numeric = "trade_value_usd") %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data3, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data4, by = c("reporter_iso", "partner_iso"))
    } else {
      data5 <- NULL
    }
    
    if (complementary_files[5] != 0) {
      data6 <- fread2(complementary_files[5], character = "commodity_code",  numeric = "trade_value_usd") %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data3, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data4, by = c("reporter_iso", "partner_iso")) %>% 
        anti_join(data5, by = c("reporter_iso", "partner_iso"))
    } else {
      data6 <- NULL
    }
    
    data <- bind_rows(data, data2, data3, data4, data5, data6) %>%
      mutate(commodity_code_parent = str_sub(commodity_code, 1, 4)) %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      mutate(parent_count = n()) %>% 
      ungroup()
    
    rm(data2, data3, data4, data5, data6)
    
    data_unrepeated_parent <- data %>% 
      filter(parent_count == 1) %>% 
      select(reporter_iso, partner_iso, commodity_code, commodity_code_parent, trade_value_usd)
    
    data_repeated_parent <- data %>% 
      filter(
        parent_count > 1,
        str_length(commodity_code == 6) | commodity_code == "9999"
      ) %>% 
      group_by(reporter_iso, partner_iso, commodity_code, commodity_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      select(reporter_iso, partner_iso, commodity_code, commodity_code_parent, trade_value_usd)
    
    rm(data)
    
    data_unrepeated_parent_summary <- data_unrepeated_parent %>% 
      filter(str_length(commodity_code_parent) == 6) %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      rename(commodity_code = commodity_code_parent)
    
    data_repeated_parent_summary <- data_repeated_parent %>% 
      filter(commodity_code_parent != "9999") %>% 
      group_by(reporter_iso, partner_iso, commodity_code_parent) %>% 
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>% 
      ungroup() %>% 
      rename(commodity_code = commodity_code_parent)
    
    data <- data_unrepeated_parent %>% 
      bind_rows(data_repeated_parent) %>% 
      bind_rows(data_unrepeated_parent_summary) %>% 
      bind_rows(data_repeated_parent_summary) %>% 
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd) %>% 
      arrange(reporter_iso, partner_iso, commodity_code) %>% 
      mutate(
        year = years_full[t],
        commodity_code_length = str_length(commodity_code)
      ) %>% 
      select(year, reporter_iso, partner_iso, commodity_code, commodity_code_length, trade_value_usd) %>% 
      filter(trade_value_usd > 0)
  
    fwrite(data, str_replace(z[t], ".gz", ""))
    compress_gz(str_replace(z[t], ".gz", ""))
  }
}
