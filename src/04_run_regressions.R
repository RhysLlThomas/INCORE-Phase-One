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

sexes <- c("M", "F")

for (reg in reg_names) {
  
  # Getting filename as combination of regression level and regression equation
  filename <- paste0(reg_level, "_", reg)
  
 # Loop over SEX 
for (sex in sexes) {
    print(paste0("Loading data for ", reg_level, " ", reg, " regression (sex: ", sex, ")."))

    # Load only parquet files for this sex (pattern matches _M_ or _F_)
    reg_dir <- file.path(indir, paste0(filename, ".parquet"))
    all_files <- list.files(reg_dir,
                            pattern = paste0("(_", sex, "_)|(_", sex, "\\.parquet$)"),
                            full.names = TRUE)

    if (length(all_files) == 0) {
      print(paste0("No parquet files found for ", filename, " (sex: ", sex, "). Skipping."))
      next
    }

# Import all files for this sex into H2O robustly (import first then rbind remaining)
data <- h2o.importFile(path = all_files[1])
if (length(all_files) > 1) {
  for (f in all_files[-1]) {
    tmp <- h2o.importFile(path = f)
    data <- h2o.rbind(data, tmp)
  }
}
  
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
  
# Robust sex-specific cell counts computed inside H2O
preds_h2o <- data[, predictors]
col_sums_df <- as.data.frame(h2o.colSums(preds_h2o, na.rm = TRUE))
cell_counts_vec <- as.numeric(col_sums_df[1, ])
cell_counts_df <- data.frame(
  names = colnames(preds_h2o),
  cell_count = pmin(cell_counts_vec, as.numeric(h2o.nrow(data))),
  stringsAsFactors = FALSE
)
  
  # Adding cell counts to coefficient dataframe
  result_df_LASSO <- merge(result_df_LASSO, cell_counts_df, by = "names", all = TRUE)
  
# Save cell counts and coefficients separately per sex
 write.csv(cell_counts_df,
            file.path(outdir, paste0(filename, "_", sex, "_cell_counts.csv")),
            row.names = FALSE)

write.csv(result_df_LASSO,
              file.path(outdir, paste0(filename, "_", sex, "_coefs_LASSO.csv")),
              row.names = FALSE)

  print(paste0("LASSO coefficients saved for ", reg, " regression (sex: ", sex, ")!"))
  
  # Getting model statistics from fit model
  stats_df_LASSO <- data.frame(
    model = paste0(filename, "_", sex),
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
selected_predictors <- intersect(names(coefs[coefs != 0]), predictors)

# If LASSO selected no predictors, skip GLM refit to avoid error
if (length(selected_predictors) == 0) {
  message("No predictors selected by LASSO for ", filename, " sex=", sex, " — skipping GLM.")
  # LASSO outputs and cell counts are already saved; continue to next sex.
  next
}

# Refitting a GLM model with selected predictors and no regularization
fit_GLM <- h2o.glm(
  x = selected_predictors,
  y = "los",
  training_frame = data,
  family = "gaussian",
  lambda = 0,
  compute_p_values = TRUE
)
  
# Getting coefficients from fit model and making results dataframe
result_df_GLM <- data.frame(fit_GLM@model$coefficients_table)
# Merge GLM coefficients with sex-specific cell counts (if predictor names match)
result_df_GLM <- merge(result_df_GLM, cell_counts_df, by = "names", all.x = TRUE)
# Save sex-specific GLM coefficients + counts
write.csv(result_df_GLM,
          file.path(outdir, paste0(filename, "_", sex, "_coefs_GLM.csv")),
          row.names = FALSE)
print(paste0("GLM coefficients saved for ", reg, " regression (sex: ", sex, ")!"))
  
  # Getting model statistics from fit model
  stats_df_GLM <- data.frame(
  model = paste0(filename, "_", sex),
    MSE = h2o.mse(fit_GLM),
    RMSE = h2o.rmse(fit_GLM),
    R2 = h2o.r2(fit_GLM),
    AIC = h2o.aic(fit_GLM)
  )
  
  # Writing all model stats to single csv
  stats_path_GLM <- file.path(outdir, "GLM_model_stats.csv")
  write.table(stats_df_GLM, stats_path_GLM, sep = ",", append = TRUE,
              row.names = FALSE, col.names = !file.exists(stats_path_GLM))

  h2o.rm(data)
  gc()
  
}
}
