# Open ts-yearly-data.Rproj before running this function

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
