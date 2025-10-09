rm(list = ls())
library(h2o)
library(tidyverse)
library(arrow)

# Initializing h2o
h2o.init()

# Setting regression level and equation names
# Regression level should be specified as either "admission" or "person_year"
reg_level <- "person_year"
reg_names <- c("age_eq", "condition_eq", "family_age_eq", "family_pair_eq")

# Setting input folder
indir <- file.path("data", "03_prepped_inputs")

# Creating output folder, if it doesn't already exist
outdir <- file.path("results")
dir.create(outdir, recursive = TRUE)

for (reg in reg_names) {
  
  # Getting filename as combination of regression level and regression equation
  filename <- paste0(reg_level, "_", reg)
  
  # Reading in prepared input
  print(paste0("Loading data for ", reg_level, " ", reg, " regression."))
  data <- h2o.importFile(path = file.path(indir, paste0(filename, ".parquet")))
  
  # Setting predictors as all columns except "los"
  predictors <- setdiff(colnames(data), c("los"))
  
  # Running LASSO regression, using lambda search
  print("Running regression...")
  fit <- h2o.glm(
    x = predictors,
    y = "los",
    training_frame = data,
    family = "gaussian",
    alpha = 1,
    lambda_search = TRUE
  )
  
  # Getting coefficients from fit model
  coefs <- h2o.coef(fit)
  result_df <- data.frame(
    variable = names(coefs),
    coefficient = as.numeric(coefs)
  )
  
  # Get cell counts. Since columns are encoded as 0 or 1, the sum across all rows
  # is equal to the cell count. Only the "n_admissions" column in person_year
  # level regressions is not binary, and are instead positive integers
  cell_counts <- as.vector(h2o.sum(data[predictors], return_frame=T))
  
  # Making cell counts dataframe. Using pmin to limit the cell counts to the 
  # number of rows in the dataframe. This should only be relevant for the 
  # "n_admissions" columns as noted above
  cell_counts_df <- data.frame(
    variable = names(data[predictors]),
    cell_count = pmin(cell_counts, nrow(data)),
    stringsAsFactors = FALSE
  )
  
  # Adding cell counts to coefficient dataframe
  result_df <- merge(result_df, cell_counts_df, by = "variable", all = TRUE)
  
  # Save coefficients + counts
  write.csv(result_df, file.path(outdir, paste0(filename, "_coefs.csv")), row.names = FALSE)
  print(paste0("Coefficients saved for ", reg, " regression!"))
  
  # Getting model statistics from fit model
  stats_df <- data.frame(
    model = filename,
    MSE = h2o.mse(fit),
    RMSE = h2o.rmse(fit),
    R2 = h2o.r2(fit),
    AIC = h2o.aic(fit)
  )
  
  # Writing all model stats to single csv
  stats_path <- file.path(outdir, "all_model_stats.csv")
  write.table(stats_df, stats_path, sep = ",", append = TRUE,
              row.names = FALSE, col.names = !file.exists(stats_path))
}