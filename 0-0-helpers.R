# Open ts-yearly-data.Rproj before running this function

messageline <- function() {
  message(rep("-", 60))
}

extract <- function(x,y,z,t) {
  if (file.exists(y[[t]])) {
    messageline()
    message(paste(x[[t]], "already unzipped. Skipping."))
  } else {
    messageline()
    message(paste("Unzipping", x[[t]]))
    system(sprintf("7z e -aos %s -oc:%s", x[[t]], z))
  }
}

fread2 <- function(x) {
  messageline()
  message("function fread2")
  message("x: ", x)
  as_tibble(fread(
    x,
    colClasses = list(
      character = c("Commodity Code"),
      numeric = c("Trade Value (US$)")
    )
  )) %>% clean_names()
}

file_remove <- function(x) {
  try(file.remove(x))
}

compress_gz <- function(x) {
  system(paste("gzip", x))
}