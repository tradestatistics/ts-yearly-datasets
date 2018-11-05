# Open ts-yearly-data.Rproj before running this function

clean <- function(n_cores = 4) {
  
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
  
  # helpers -----------------------------------------------------------------
  
  source("0-0-helpers.R")
  
  # uncompress input --------------------------------------------------------
  
  lapply(seq_along(raw_csv), extract, x = raw_zip, y = raw_csv, z = raw_dir)
  
  # create tidy datasets ----------------------------------------------------
  
  messageline()
  message("Rearranging files. Please wait...")
  
  if (operating_system != "Windows") {
    mclapply(seq_along(raw_csv), compute_tidy_data, mc.cores = n_cores)
  } else {
    lapply(seq_along(raw_csv), compute_tidy_data)
  }
  
  lapply(raw_csv, file_remove)
}

clean()
