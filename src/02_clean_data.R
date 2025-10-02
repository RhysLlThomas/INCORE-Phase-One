rm(list = ls())
library(tidyverse)
library(arrow)
source('src/utils.R')

#----------------
##### Setup #####
#----------------

# Getting all year/age/sex combinations
# Ignoring age_start and sex_id values of -1, which are considered invalid
years <- seq(2014, 2019, 1)
ages <- read_feather(file.path("maps", "age_groups.feather"))$age_start
sexes <- c("M", "F")
partitions <- expand.grid(year_id=years, age_start = ages, sex_id = sexes)

# Reading in dataframe containing condition families for NEC and other conditions
NEC_other_families <- read_feather(file.path("maps", "NEC_other_conditions_lookup.feather"))

# Creating a lookup of related conditions (same family) for each NEC condition
NEC_other_lookup <- NEC_other_families %>%
  group_by(NEC_or_other_condition, family) %>%
  summarise(non_NEC_or_other_condition = list(non_NEC_or_other_condition), .groups = "drop") %>%
  rowwise() %>%
  mutate(value = list(list(family = family, non_NEC_or_other_condition = non_NEC_or_other_condition))) %>%
  ungroup() %>%
  select(NEC_or_other_condition, value) %>%
  deframe()

# Read in condition details, subsetting to condition and family
condition_families <- read_feather(file.path("maps", "condition_details.feather"))[,c("condition", "family")]

# Setting input folder
indir <- file.path("data", "01_transformed_data", "transformed_data.parquet")

# Creating output folder, if it doesn't already exist
outdir <- file.path("data", "02_cleaned_data")
dir.create(outdir, recursive = TRUE)

# Loading dataset without reading fully into memory
data <- open_dataset(indir)

#------------------------
##### Cleaning data #####
#------------------------

# Iterating over all age/sex combinations
for (i in 1:nrow(partitions)){
  
  # Getting year/age/sex combination
  year <- partitions[i,'year_id']
  age <- partitions[i,'age_start']
  sex <- partitions[i,'sex_id']
  
  # Reading in data for specific year/age/sex
  df <- data %>%
    filter((year_id==!!year) & (age_start==!!age) & (sex_id==!!sex)) %>%
    as_tibble()
  
  # Skip if there's no data
  if (nrow(df) == 0 ) {
    next
  }
  
  # Set condition hierarchy
  df <- get_primary_condition(df, NEC_other_lookup=NEC_other_lookup)
  
  # Saving out primary condition counts
  primary_counts <- save_primary_counts(df, year=year, age=age, sex=sex, condition_families=condition_families)
  
  # Redistribute _NEC and _gc conditions
  df <- redistribute_conditions(df, primary_counts=primary_counts, condition_families=condition_families)
  
  #-----------------
  ##### Saving #####
  #-----------------
  
  # Selecting necessary columns and saving out data as parquet file
  # File is partitioned by sex_id and age_start, maximum of 2.5M rows per file
  df %>%
    select(bene_id, admission_id, year_id, sex_id, age_start, icd_level, condition, family, is_primary, los) %>%
    group_by(year_id, age_start, sex_id) %>%
    write_dataset(file.path(outdir, "cleaned_data.parquet"),
                  basename_template=paste(c(year, age, sex, "{{i}}.parquet"), collapse='_'),
                  max_rows_per_file = 2.5e6L)
  
}
