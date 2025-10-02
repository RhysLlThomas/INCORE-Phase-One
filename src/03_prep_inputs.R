rm(list = ls())
library(data.table)
library(arrow)
source('src/utils.R')

#----------------
##### Setup #####
#----------------

# Getting all year/age/sex combinations
# Ignoring age_start and sex_id values of -1, which are considered invalid
years <- seq(2014, 2019, 1)
ages <- read_feather("maps/age_groups.feather")$age_start
sexes <- c("M", "F")
partitions <- expand.grid(year_id=years, age_start = ages, sex_id = sexes)

# Getting all conditions and condition families
condition_details <- read_feather(file.path("maps", "condition_details.feather")) %>%
  filter(!endsWith(condition, '_NEC'))
conditions <- condition_details  %>%
  pull(condition) %>%
  unique()
families <- condition_details %>%
  pull(family) %>%
  unique()

# Setting regression equation names for saving
reg_names <- c("age_eq", "condition_eq", "family_age_eq", "family_pair_eq")

# Setting input folder
indir <- file.path("data", "02_cleaned_data", "cleaned_data.parquet")

# Creating output folders, if they don't already exist
outdir <- file.path("data", "03_prepped_inputs")
for (reg in reg_names){
  dir.create(paste0(file.path(outdir, reg), ".parquet"),
             recursive = TRUE)
}

# Loading dataset without reading fully into memory
data <- open_dataset(indir)

#--------------------------
##### Prepping inputs #####
#--------------------------

for (i in 1:nrow(partitions)) {
  
  # Getting year/age/sex combination
  year <- partitions[i,'year_id']
  age <- partitions[i,'age_start']
  sex <- partitions[i,'sex_id']
  
  # Reading in data for specific year/age/sex as a datatable
  DT <- data %>%
    filter((year_id==!!year) & (age_start==!!age) & (sex_id==!!sex)) %>%
    as.data.table()
  
  # Skip if there's no data
  if (nrow(DT) == 0) {
    next
  }
  
  # Creating regression matrices
  reg_matrices <- create_reg_matrices(DT, years, ages, conditions, families)
  
  # For each equation...
  for (reg in reg_names) {
    
    # Write out data in chunks, to help with memory usage
    # Defaults to 100,000 rows per chunk
    chunked_save(
      reg_matrices[[reg]],
      file.path(outdir, paste0(reg, ".parquet"), paste0(paste(c(year, age, sex), collapse = '_'), '.parquet')))
  }
  
}


