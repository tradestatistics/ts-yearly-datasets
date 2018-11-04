# Open ts-yearly-data.Rproj before running this function

download <- function(n_cores = 4) {
  # user parameters ---------------------------------------------------------

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
  
  remove_old_files <- menu(
    c("yes", "no"),
    title = "Remove old files (y/n):",
    graphics = F
  )
  
  token <- menu(
    c("yes", "no"),
    title = "Have you stored your token safely in your .Renviron file?",
    graphics = F
  )
  
  stopifnot(token == 1)
  
  token <- Sys.getenv("token")
  
  # helpers -----------------------------------------------------------------
  
  source("0-0-helpers.R")

  # download data -----------------------------------------------------------

  try(
    old_file <- list.files(raw_dir, pattern = "downloaded", full.names = T)
  )

  if (length(old_file) > 0) {
    old_links <- as_tibble(fread(old_file)) %>%
      mutate(
        local_file_date = gsub(".*pub-", "", file),
        local_file_date = gsub("_fmt.*", "", local_file_date),
        local_file_date = as.Date(local_file_date, "%Y%m%d")
      ) %>%
      rename(old_file = file)
  }

  links <- tibble(
    year = years,
    url = paste0(
      "https://comtrade.un.org/api/get/bulk/C/A/",
      year,
      "/ALL/",
      classification2,
      "?token=",
      token
    ),
    file = NA
  )

  files <- fromJSON(sprintf("https://comtrade.un.org/api/refs/da/bulk?freq=A&r=ALL&px=%s&token=%s", classification2, token)) %>%
    filter(ps %in% years) %>%
    arrange(ps)

  if (exists("old_links")) {
    links <- links %>%
      mutate(file = paste0(raw_dir, "/", files$name)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      left_join(old_links %>% select(-url), by = "year") %>%
      rename(new_file = file) %>%
      mutate(
        server_file_date = as.Date(
          ifelse(is.na(local_file_date), server_file_date + 1, server_file_date),
          origin = "1970-01-01"
        ),
        local_file_date = as.Date(
          ifelse(is.na(local_file_date), server_file_date - 1, local_file_date),
          origin = "1970-01-01"
        )
      )
  } else {
    links <- links %>%
      mutate(file = paste0(raw_dir, "/", files$name)) %>%
      mutate(
        server_file_date = gsub(".*pub-", "", file),
        server_file_date = gsub("_fmt.*", "", server_file_date),
        server_file_date = as.Date(server_file_date, "%Y%m%d")
      ) %>%
      mutate(old_file = NA) %>% # trick in case there are no old files
      mutate(local_file_date = server_file_date) %>%
      mutate(server_file_date = as.Date(server_file_date + 1, origin = "1970-01-01")) %>% # trick in case there are no old files
      rename(new_file = file)
  }

  if (operating_system != "Windows") {
    mclapply(seq_along(years), data_downloading, mc.cores = n_cores)
  } else {
    lapply(seq_along(years), data_downloading)
  }
  
  links <- links %>%
    mutate(url = gsub(token, "REPLACE_TOKEN", url)) %>%
    select(year, url, new_file, local_file_date) %>%
    rename(file = new_file)

  try(file.remove(old_file))
  fwrite(links, paste0(raw_dir, "/downloaded-files-", Sys.Date(), ".csv"))
}

download()
