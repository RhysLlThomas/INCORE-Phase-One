rm(list = ls())
library(data.table)
library(arrow)
source(file.path("src","utils.R"))

#----------------
##### Setup #####
#----------------

# Getting all year/age/sex combinations
# Ignoring age_start and sex_id values of -1, which are considered invalid
years <- seq(2014, 2019, 1)
ages <- read_feather(file.path("maps","age_groups.feather"))$age_start
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

# Setting regression level and equation names
# Regression level should be specified as either "admission" or "person_year"
reg_level <- "person_year"
reg_names <- c("age_eq", "condition_eq", "family_age_eq", "family_pair_eq")

# Setting input and output folders
indir <- file.path("data", "02_cleaned_data", "cleaned_data.parquet")
outdir <- file.path("data", "03_prepped_inputs")

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
  reg_matrices <- create_reg_matrices(DT, years, ages, conditions, families, level=reg_level)
  
  # For each equation...
  for (reg in reg_names) {
    
    # Setting directory name and file name. Directory is unique for each
    # regression level/equation while filename is unique for each year/age/sex
    dir_name <- paste0(reg_level, "_", reg, ".parquet")
    file_name <- paste0(paste(c(year, age, sex), collapse = '_'), '.parquet')
    
    # Create output directory for current regression equation
    if (!dir.exists(file.path(outdir, dir_name))) {
      dir.create(file.path(outdir, dir_name),
                 recursive = TRUE)
      }
    
    # Write out data in chunks, to help with memory usage
    # Defaults to 100,000 rows per chunk
    chunked_save(
      reg_matrices[[reg]],
      file.path(outdir, dir_name, file_name))
  }
  
}
