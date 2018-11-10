# Open ts-yearly-data.Rproj before running this function

join_datasets <- function(x, y, z, t) {
  converted_codes <- c1[dataset2]
  
  if (!file.exists(grep(paste0(converted_codes, "-", years_all_classifications[t]), z, value = T))) {
    if (years_all_classifications[t] < 1976) {
      f1 <- NULL
      f2 <- NULL
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f6 <- NULL
    }
    
    if (years_all_classifications[t] >= 1976 & years_all_classifications[t] < 1992) {
      f1 <- NULL
      f2 <- NULL
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
    }
    
    if (years_all_classifications[t] >= 1992 & years_all_classifications[t] < 1996) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f2 <- NULL
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
    }
    
    if (years_all_classifications[t] >= 1996 & years_all_classifications[t] < 2002) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f2 <- ifelse(converted_codes == c1[2], 
                   grep(paste0(c1[2], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[2], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f3 <- NULL
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
    }
    
    if (years_all_classifications[t] >= 2002 & years_all_classifications[t] < 2007) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f2 <- ifelse(converted_codes == c1[2], 
                   grep(paste0(c1[2], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[2], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f3 <- ifelse(converted_codes == c1[3], 
                   grep(paste0(c1[3], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[3], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f4 <- NULL
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
    }
    
    if (years_all_classifications[t] >= 2007) {
      f1 <- ifelse(converted_codes == c1[1], 
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[1], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f2 <- ifelse(converted_codes == c1[2], 
                   grep(paste0(c1[2], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[2], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f3 <- ifelse(converted_codes == c1[3], 
                   grep(paste0(c1[3], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[3], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f4 <- ifelse(converted_codes == c1[4], 
                   grep(paste0(c1[4], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[4], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f5 <- ifelse(converted_codes == c1[5], 
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[5], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
      )
      f6 <- ifelse(converted_codes == c1[6], 
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), x, value = T),
                   grep(paste0(c1[6], "-", years_all_classifications[t], ".csv.gz"), z, value = T)
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
    
    data <- fread2(leading_file, char = c("commodity_code"), num = c("trade_value_usd"))
    
    if (complementary_files[1] != 0) {
      data2 <- fread2(complementary_files[1], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        anti_join(data, by = c("reporter_iso", "partner_iso"))
    } else {
      data2 <- NULL
    }
    
    if (complementary_files[2] != 0) {
      data3 <- fread2(complementary_files[2], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso"))
    } else {
      data3 <- NULL
    }
    
    if (complementary_files[3] != 0) {
      data4 <- fread2(complementary_files[3], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data3, by = c("reporter_iso", "partner_iso"))
    } else {
      data4 <- NULL
    }
    
    if (complementary_files[4] != 0) {
      data5 <- fread2(complementary_files[4], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data3, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data4, by = c("reporter_iso", "partner_iso"))
    } else {
      data5 <- NULL
    }
    
    if (complementary_files[5] != 0) {
      data6 <- fread2(complementary_files[5], char = c("commodity_code"), num = c("trade_value_usd")) %>%
        anti_join(data, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data2, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data3, by = c("reporter_iso", "partner_iso")) %>%
        anti_join(data4, by = c("reporter_iso", "partner_iso")) %>% 
        anti_join(data5, by = c("reporter_iso", "partner_iso"))
    } else {
      data6 <- NULL
    }
    
    data <- bind_rows(data, data2, data3, data4, data5, data6) %>%
      arrange(reporter_iso, partner_iso, commodity_code)
    
    rm(data2, data3, data4, data5, data6)
    
    fwrite(data, paste0(converted_dir, "/", c1[4], "/", c1[4], "-", years_all_classifications[t], ".csv"))
    compress_gz(paste0(converted_dir, "/", c1[4], "/", c1[4], "-", years_all_classifications[t], ".csv"))
  }
}
