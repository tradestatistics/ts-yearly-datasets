# Open ts-yearly-data.Rproj before running this function

# user input --------------------------------------------------------------

dataset <- menu(
  c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
  title = "Select dataset:",
  graphics = F
)

if (exists("ask_for_convertion_codes")) {
  dataset2 <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
    title = "Convert codes to:",
    graphics = F
  )
}

if (exists("ask_for_converted_codes")) {
  dataset2 <- menu(
    c("HS rev 1992", "HS rev 1996", "HS rev 2002", "HS rev 2007", "SITC rev 1", "SITC rev 2", "SITC rev 3", "SITC rev 4"),
    title = "Converted codes:",
    graphics = F
  )  
}

# classification ----------------------------------------------------------

if (dataset < 5) {
  classification <- "hs"
} else {
  classification <- "sitc"
}

# number of digits --------------------------------------------------------

if (classification == "sitc") {
  J <- c(4,5) 
} else {
  J <- c(4,6)
}

# years by classification -------------------------------------------------

if (dataset == 1) { revision <- 1992; revision2 <- revision; classification2 <- "H0" }
if (dataset == 2) { revision <- 1996; revision2 <- revision; classification2 <- "H1" }
if (dataset == 3) { revision <- 2002; revision2 <- revision; classification2 <- "H2" }
if (dataset == 4) { revision <- 2007; revision2 <- revision; classification2 <- "H3" }
if (dataset == 5) { revision <- 1; revision2 <- 1962; classification2 <- "S1" }
if (dataset == 6) { revision <- 2; revision2 <- 1976; classification2 <- "S2" }
if (dataset == 7) { revision <- 3; revision2 <- 1988; classification2 <- "S3" }
if (dataset == 8) { revision <- 4; revision2 <- 2007; classification2 <- "S4" }

years <- revision2:2016
years_all_classifications <- 1962:2016

years_missing_t_minus_1 <- 1962
years_missing_t_minus_2 <- 1963
years_missing_t_minus_5 <- 1963:1966
years_full <- 1967:2016

#years_sitc_rev1 <- 1962:1991 # because SITC rev1 is used just to complete SITC rev2 data
