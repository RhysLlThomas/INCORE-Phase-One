rm(list = ls())
library(tidyverse)
library(arrow)
library(tools)
source(file.path("src","utils.R"))

#----------------------
##### USER INPUTS #####
#----------------------

filepath <- "INPUT DATA FILEPATH"

# Existing beneficiary ID column should be unique to individuals by year.
# Individuals can appear multiple times in the data, but beneficiary ID should
# be the same for a single individual
beneficiary_id_col <- "BENE_ID" 

# Existing admission ID column is not necessary, but can be used to assign admission IDs
# Data should be unique on admission ID, that is, one row per admission
admission_id_cols <- NULL 

# Year can be provided as a standalone column or can be extracted from a 
# discharge date column
year_col <- "YEAR"
discharge_date_col <- NULL

# Age can be provided as a standalone column or can be extracted from both a
# birth date column and admission date column to find age at admission
age_col <-"AGE"
birth_date_col <- NULL
admission_date_col <- NULL

# Sex is expected to be coded in a single column. The values of sex_col_map 
# should be modified to correspond to how sex is encoded in the data

# For example, if the data contains "man" for male and "woman" for female...
# sex_col_map <- list('male'='M', 'female'='F')

# Or, if the data contains "1" for male and "2" for female...
# sex_col_map <- list('1'='M', '2'='F')
sex_col <- "FEMALE"
sex_col_map <- list('0'='M', '1'='F')

# ICD version can be provided as a standalone column or can be extracted from a
# discharge date column (we expect any admission discharge after 1st October,
# 2015 to be ICD 10, and anything before to be ICD 9). If you supply a icd_ver_col
# you must also provide a map, icd_ver_col_map, to encoded the values correctly.

# For example, if the data contains "10" for ICD 10 and "9" for ICD 9...
# icd_ver_col_map <- list('10'='icd10', '9'='icd9')

# Or, if the data contains "v10" for ICD 10 and "v9" for ICD 9...
# icd_ver_col_map <- list('v10'='icd10', 'v9'='icd9')
icd_ver_col <- NULL #"DXVER"
icd_ver_col_map <- NULL #list('10'='icd10', '9'='icd9')

# Length of stay can be provided as a standalone column or can be extracted from 
# both a discharge date column and an admission date column.
los_col <- "LOS"

# ICD codes corresponding to an admission should be provided as multiple columns.
# You can specify each column, or use regular expressions (regex) to find all
# columns using a matching pattern. If you provided each column explicitly,
# Ensure that they are in order.
icd_cols <- c("^I10_DX\\d+$*")

#----------------
##### Setup #####
#----------------

# Read in ICD to condition mapping
icd_condition_map <- read_feather(file.path("maps", "icd_map.feather"))

# Creating output folder, if it doesn't already exist
outdir <- file.path("data", "01_transformed_data")
dir.create(outdir, recursive = TRUE)

# Read in raw data, using custom function to handle various file types
# Supports reading .csv, .parquet, .dta, .sav, .sas7bdat, .xls, .xlsx, and .rds
df <- read_data(filepath)

# Subsetting data to specified columns only
df <- subset_cols(df, select_cols=c(beneficiary_id_col, admission_id_cols, year_col, age_col, sex_col,
                                    discharge_date_col, admission_date_col,
                                    birth_date_col,icd_ver_col, los_col, icd_cols))
#---------------------
##### Processing #####
#---------------------

# Create column for bene_id
df <- get_unique_id(df, unique_id_cols=beneficiary_id_col, col_name="bene_id")

# Create column for admission_id
df <- get_unique_id(df, unique_id_cols=admission_id_cols, col_name="admission_id")

# Create column for year_id
df <- get_year_id(df, year_col=year_col, discharge_date_col=discharge_date_col)

# Create column for age_start
df <- get_age_bins(df, age_col=age_col, birth_date_col=birth_date_col, admission_date_col=admission_date_col)

# Create column for sex_id
df <- get_sexes(df, sex_col=sex_col, sex_col_map=sex_col_map)

# Create column for icd_ver
df <- get_icd_version(df, icd_ver_col=icd_ver_col, icd_ver_col_map=icd_ver_col_map, discharge_date_col=discharge_date_col)

# Create column for los
df <- get_length_of_stay(df, los_col=los_col, discharge_date_col=discharge_date_col, admission_date_col=admission_date_col)

# Map ICD codes to conditions
df <- get_conditions(df, icd_cols=icd_cols, icd_condition_map=icd_condition_map)

#-----------------
##### Saving #####
#-----------------

# Selecting necessary columns and saving out data as parquet file
# File is partitioned by sex_id and age_start, maximum of 2.5M rows per file
df %>%
  select(bene_id, admission_id, year_id, sex_id, age_start, icd_ver, icd_level, icd_code, condition, los) %>%
  group_by(year_id, age_start, sex_id) %>%
  write_dataset(file.path(outdir,'transformed_data.parquet'),
                basename_template=paste0(file_path_sans_ext(basename(filepath)),'_{{i}}.parquet'),
                max_rows_per_file = 2.5e6L)