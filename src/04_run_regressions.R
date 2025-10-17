rm(list = ls())
library(h2o)
library(tidyverse)
library(arrow)

# Initializing h2o
h2o.init()

# Setting regression level and equation names
# Regression level should be specified as either "admission" or "person_year"
reg_level <- "admission"
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
  fit_LASSO <- h2o.glm(
    x = predictors,
    y = "los",
    training_frame = data,
    family = "gaussian",
    alpha = 1,
    lambda_search = TRUE
  )
  
  # Getting coefficients from fit model and making results dataframe
  coefs <- h2o.coef(fit_LASSO)
  result_df_LASSO <- data.frame(fit_LASSO@model$coefficients_table)
  
  # Get cell counts. Since columns are encoded as 0 or 1, the sum across all rows
  # is equal to the cell count. Only the "n_admissions" column in person_year
  # level regressions is not binary, and are instead positive integers
  cell_counts <- as.vector(h2o.sum(data[predictors], return_frame=T))
  
  # Making cell counts dataframe. Using pmin to limit the cell counts to the 
  # number of rows in the dataframe. This should only be relevant for the 
  # "n_admissions" columns as noted above
  cell_counts_df <- data.frame(
    names = names(data[predictors]),
    cell_count = pmin(cell_counts, nrow(data)),
    stringsAsFactors = FALSE
  )
  
  # Adding cell counts to coefficient dataframe
  result_df_LASSO <- merge(result_df_LASSO, cell_counts_df, by = "names", all = TRUE)
  
  # Save coefficients + counts
  write.csv(result_df_LASSO, file.path(outdir, paste0(filename, "_coefs_LASSO.csv")), row.names = FALSE)
  print(paste0("LASSO coefficients saved for ", reg, " regression!"))
  
  # Getting model statistics from fit model
  stats_df_LASSO <- data.frame(
    model = filename,
    MSE = h2o.mse(fit_LASSO),
    RMSE = h2o.rmse(fit_LASSO),
    R2 = h2o.r2(fit_LASSO),
    AIC = h2o.aic(fit_LASSO)
  )
  
  # Writing all model stats to single csv
  stats_path_LASSO <- file.path(outdir, "LASSO_model_stats.csv")
  write.table(stats_df_LASSO, stats_path_LASSO, sep = ",", append = TRUE,
              row.names = FALSE, col.names = !file.exists(stats_path_LASSO))
  
  # Selecting predictors that were not dropped in LASSO regression
  selected_predictors <- intersect(names(coefs[coefs!=0]), predictors)
  
  # Refitting a GLM model with selected predictors and no regularization
  fit_GLM <- h2o.glm(
    x = selected_predictors,
    y = "los",
    training_frame = data,
    family = "gaussian",
    lambda=0,
    compute_p_values = TRUE
  )
  
  # Getting coefficients from fit model and making results dataframe
  result_df_GLM <- data.frame(fit_GLM@model$coefficients_table)
  
  # Save coefficients + counts
  write.csv(result_df_GLM, file.path(outdir, paste0(filename, "_coefs_GLM.csv")), row.names = FALSE)
  print(paste0("GLM coefficients saved for ", reg, " regression!"))
  
  # Getting model statistics from fit model
  stats_df_GLM <- data.frame(
    model = filename,
    MSE = h2o.mse(fit_GLM),
    RMSE = h2o.rmse(fit_GLM),
    R2 = h2o.r2(fit_GLM),
    AIC = h2o.aic(fit_GLM)
  )
  
  # Writing all model stats to single csv
  stats_path_GLM <- file.path(outdir, "GLM_model_stats.csv")
  write.table(stats_df_GLM, stats_path_GLM, sep = ",", append = TRUE,
              row.names = FALSE, col.names = !file.exists(stats_path_GLM))
  
}
