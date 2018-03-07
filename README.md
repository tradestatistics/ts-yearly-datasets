
<!-- README.md is generated from README.Rmd. Please edit that file -->
Description
-----------

Scripts to recreate the trade data available at the [Observatory of Economic Complexity](http://atlas.media.mit.edu/en/).

We have a repo explaining the methodological details. Please visit [OEC Documentation](https://observatory-economic-complexity.github.io/oec-documentation/) and the [Official website](http://atlas.media.mit.edu/en/) for the details.

These scripts are released under MIT license.

How to use this project
-----------------------

While you can run the scripts from UNIX terminal, we highly recommend [RStudio](https://www.rstudio.com/).

Each project is organized as an RStudio project. Each project contains a master script titled `0-some-function.R` at the top level but some projects such as [OEC Product Space](https://github.com/observatory-economic-complexity/oec-product-space) are organized in subfolders where each subfolder contains a master script.

The master script, for any case, runs a function with user prompt and/or simple scripts. The safe way to run this without errors is to open the project and then run the master script from RStudio.

[OEC Packages Snapshot](https://github.com/observatory-economic-complexity/oec-packages-snapshot) contains a bundle to avoid dependencies or packages related problems, but this project already contains an unbundled set of packages to ease reproducibility.

You can read more about RStudio projects [here](https://support.rstudio.com/hc/en-us/articles/200526207-Using-Projects).

Project structure
-----------------

    oec-yearly-datasets
    └── README.md
    └── README.Rmd
    └── 1-raw-data
    |    └── 0-download-function.R
    |    └── hs-rev1992
    |    └── hs-rev1996
    |    └── hs-rev2002
    |    └── hs-rev2007
    |    └── hs-rev2012
    |    └── sitc-rev2
    └── 2-processed-data
    |    └── 0-organize-function.R
    |    └── hs-rev1992
    |    └── hs-rev1996
    |    └── hs-rev2002
    |    └── hs-rev2007
    |    └── hs-rev2012
    |    └── sitc-rev2
    └── packrat

Output
------

The scripts generates different files organized under `1-raw-data/classification-rev-bulk/` (i.e `1-raw-data/hs-rev1992/`) and `2-processed-data/classification-rev/`.

### Generated files (HS 92 4 digits, 2015)

#### Raw data

    ## Skim summary statistics  
    ##  n obs: 25248037    
    ##  n variables: 22    
    ## 
    ## Variable type: character
    ## 
    ## variable         missing   complete   n          min   max   empty    n_unique 
    ## ---------------  --------  ---------  ---------  ----  ----  -------  ---------
    ## Classification   0         25248037   25248037   2     2     0        1        
    ## Commodity        0         25248037   25248037   3     71    0        5934     
    ## Commodity Code   0         25248037   25248037   2     6     0        6204     
    ## Partner          0         25248037   25248037   3     44    0        244      
    ## Partner ISO      0         25248037   25248037   0     3     565780   235      
    ## Qty Unit         0         25248037   25248037   11    48    0        12       
    ## Reporter         0         25248037   25248037   3     32    0        152      
    ## Reporter ISO     0         25248037   25248037   0     3     313798   152      
    ## Trade Flow       0         25248037   25248037   6     9     0        4        
    ## 
    ## Variable type: integer
    ## 
    ## variable          missing   complete   n          mean     sd       p0     p25    median   p75    p100   hist     
    ## ----------------  --------  ---------  ---------  -------  -------  -----  -----  -------  -----  -----  ---------
    ## Aggregate Level   0         25248037   25248037   5.19     1.21     0      4      6        6      6      ▁▁▁▁▁▃▁▇ 
    ## Flag              0         25248037   25248037   0.98     2.01     0      0      0        0      6      ▇▁▁▁▁▁▁▁ 
    ## Is Leaf Code      0         25248037   25248037   0.66     0.48     0      0      1        1      1      ▅▁▁▁▁▁▁▇ 
    ## Partner Code      0         25248037   25248037   429.88   268.53   0      203    410      699    899    ▇▆▅▇▅▅▇▆ 
    ## Period            0         25248037   25248037   2015     0        2015   2015   2015     2015   2015   ▁▁▁▇▁▁▁▁ 
    ## Period Desc.      0         25248037   25248037   2015     0        2015   2015   2015     2015   2015   ▁▁▁▇▁▁▁▁ 
    ## Qty Unit Code     0         25248037   25248037   6.15     2.76     1      5      8        8      13     ▂▁▂▁▇▁▁▁ 
    ## Reporter Code     0         25248037   25248037   449.48   255.91   4      222    450      699    894    ▆▆▅▇▆▅▇▆ 
    ## Trade Flow Code   0         25248037   25248037   1.54     0.59     1      1      1        2      4      ▇▁▇▁▁▁▁▁ 
    ## Year              0         25248037   25248037   2015     0        2015   2015   2015     2015   2015   ▁▁▁▇▁▁▁▁ 
    ## 
    ## Variable type: numeric
    ## 
    ## variable            missing   complete   n          mean         sd        p0   p25    median   p75      p100      hist     
    ## ------------------  --------  ---------  ---------  -----------  --------  ---  -----  -------  -------  --------  ---------
    ## Netweight (kg)      2172664   23075373   25248037   4472256.17   7.7e+08   0    80     1500     27885    9.5e+11   ▇▁▁▁▁▁▁▁ 
    ## Qty                 4782092   20465945   25248037   1.9e+08      2.4e+11   0    52     1152     25711    7.4e+14   ▇▁▁▁▁▁▁▁ 
    ## Trade Value (US$)   0         25248037   25248037   1.1e+07      1.1e+09   1    1999   22628    253633   2.3e+12   ▇▁▁▁▁▁▁▁

#### Verified data

    ## Skim summary statistics  
    ##  n obs: 11545506    
    ##  n variables: 9    
    ## 
    ## Variable type: character
    ## 
    ## variable         missing   complete   n          min   max   empty   n_unique 
    ## ---------------  --------  ---------  ---------  ----  ----  ------  ---------
    ## commodity_code   0         11545506   11545506   6     6     0       4865     
    ## partner_iso      0         11545506   11545506   3     3     0       232      
    ## reporter_iso     0         11545506   11545506   3     3     0       151      
    ## 
    ## Variable type: integer
    ## 
    ## variable   missing   complete   n          mean   sd   p0     p25    median   p75    p100   hist     
    ## ---------  --------  ---------  ---------  -----  ---  -----  -----  -------  -----  -----  ---------
    ## year       0         11545506   11545506   2015   0    2015   2015   2015     2015   2015   ▁▁▁▇▁▁▁▁ 
    ## 
    ## Variable type: numeric
    ## 
    ## variable     missing    complete   n          mean         sd        p0   p25    median   p75      p100      hist     
    ## -----------  ---------  ---------  ---------  -----------  --------  ---  -----  -------  -------  --------  ---------
    ## export_kg    4118225    7427281    11545506   1557982.42   3.9e+08   0    70     1063     17675    7.7e+11   ▇▁▁▁▁▁▁▁ 
    ## export_usd   3958534    7586972    11545506   2e+06        7.2e+07   1    1677   16132    150973   7.3e+10   ▇▁▁▁▁▁▁▁ 
    ## import_kg    3814646    7730860    11545506   1655173.13   3.9e+08   0    50     813      14662    7.6e+11   ▇▁▁▁▁▁▁▁ 
    ## import_usd   3623387    7922119    11545506   2e+06        7.1e+07   1    1112   11779    120879   5.7e+10   ▇▁▁▁▁▁▁▁ 
    ## marker       10752845   792661     11545506   1.57         0.5       1    1      2        2        3         ▆▁▁▇▁▁▁▁

For each NA or 0 import/export value we tried to fill the gap. If country A reported NA or 0 exports (imports) of product B to (from) country C, then we searched what country C reported of imports (exports) of product B from (to) country A. The column `marker` indicated those replacements under this labels:

    |marker|meaning                              |
    |------|-------------------------------------|
    |1     |imports with replacements            |
    |2     |exports with replacements            |
    |3     |imports and exports with replacements|
    |NA    |no replacements needed               |

Software details
----------------

Here the version information about R, the OS and attached or loaded packages for this project:

    ## R version 3.4.3 (2017-11-30)
    ## Platform: x86_64-pc-linux-gnu (64-bit)
    ## Running under: Ubuntu 16.04.3 LTS
    ## 
    ## Matrix products: default
    ## BLAS/LAPACK: /opt/intel/compilers_and_libraries_2017.5.239/linux/mkl/lib/intel64_lin/libmkl_gf_lp64.so
    ## 
    ## locale:
    ##  [1] LC_CTYPE=en_US.UTF-8       LC_NUMERIC=C              
    ##  [3] LC_TIME=en_US.UTF-8        LC_COLLATE=en_US.UTF-8    
    ##  [5] LC_MONETARY=en_US.UTF-8    LC_MESSAGES=en_US.UTF-8   
    ##  [7] LC_PAPER=en_US.UTF-8       LC_NAME=C                 
    ##  [9] LC_ADDRESS=C               LC_TELEPHONE=C            
    ## [11] LC_MEASUREMENT=en_US.UTF-8 LC_IDENTIFICATION=C       
    ## 
    ## attached base packages:
    ## [1] parallel  stats     graphics  grDevices utils     datasets  methods  
    ## [8] base     
    ## 
    ## other attached packages:
    ##  [1] bindrcpp_0.2        doParallel_1.0.11   iterators_1.0.9    
    ##  [4] foreach_1.4.4       janitor_0.3.1       tidyr_0.8.0        
    ##  [7] dplyr_0.7.4         feather_0.3.1       data.table_1.10.4-3
    ## [10] pacman_0.4.6       
    ## 
    ## loaded via a namespace (and not attached):
    ##  [1] Rcpp_0.12.15     knitr_1.20       bindr_0.1        magrittr_1.5    
    ##  [5] hms_0.4.1        tidyselect_0.2.4 R6_2.2.2         rlang_0.2.0     
    ##  [9] highr_0.6        skimr_1.0.1      stringr_1.3.0    tools_3.4.3     
    ## [13] packrat_0.4.8-1  htmltools_0.3.6  yaml_2.1.17      rprojroot_1.3-2 
    ## [17] digest_0.6.15    assertthat_0.2.0 tibble_1.4.2     purrr_0.2.4     
    ## [21] codetools_0.2-15 glue_1.2.0       evaluate_0.10.1  rmarkdown_1.9   
    ## [25] stringi_1.1.6    pander_0.6.1     compiler_3.4.3   pillar_1.2.1    
    ## [29] backports_1.1.2  pkgconfig_2.0.1

The MIT License
---------------

Copyright (c) 2017, Datawheel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
