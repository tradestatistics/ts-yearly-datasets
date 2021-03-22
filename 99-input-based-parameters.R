# classification ----------------------------------------------------------

if (dataset < 5) {
  classification <- "hs"
} else {
  classification <- "sitc"
}

# number of digits --------------------------------------------------------

if (classification == "sitc") {
  J <- c(4, 5)
} else {
  J <- c(4, 6)
}

# years by classification -------------------------------------------------

if (dataset == 1) {
  revision <- 1992
  revision2 <- revision
  classification2 <- "H0"
}
if (dataset == 2) {
  revision <- 1996
  revision2 <- revision
  classification2 <- "H1"
}
if (dataset == 3) {
  revision <- 2002
  revision2 <- revision
  classification2 <- "H2"
}
if (dataset == 4) {
  revision <- 2007
  revision2 <- revision
  classification2 <- "H3"
}
if (dataset == 5) {
  revision <- 1
  revision2 <- 1962
  classification2 <- "S1"
}
if (dataset == 6) {
  revision <- 2
  revision2 <- 1976
  classification2 <- "S2"
}
if (dataset == 7) {
  revision <- 3
  revision2 <- 1988
  classification2 <- "S3"
}
if (dataset == 8) {
  revision <- 4
  revision2 <- 2007
  classification2 <- "S4"
}

max_year <- 2020
years <- revision2:max_year
years_full <- 1962:max_year

years_missing_t_minus_1 <- 1962
years_missing_t_minus_2 <- 1963
years_missing_t_minus_5 <- 1963:1966
years_with_two_previous_recordings <- 1967:max_year
