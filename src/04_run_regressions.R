rm(list = ls())
library(h2o)

# Initializing h2o
h2o.init()

# Setting regression equation names for saving
reg_names <- c("age_eq", "condition_eq", "family_age_eq", "family_pair_eq")

# Setting input folder
indir <- file.path("data", "03_prepped_inputs")

# Creating output folder, if it doesn't already exist
outdir <- file.path("results")
dir.create(outdir, recursive = TRUE)

for (reg in reg_names) {
  
  # Loading equation parquet file as an h2o frame
  print(paste0("Loading data for ", reg, " regression."))
  data <- h2o.importFile(path = file.path(indir, paste0(reg, ".parquet")))
  
  # Setting predictor columns
  predictors <- setdiff(colnames(data), c("los"))
  
  # Running LASSO regression
  print("Running regression...")
  fit <- h2o.glm(
    x = predictors,
    y = "los",
    training_frame = data,
    family = "gaussian",
    alpha = 1, # alpha = 1 corresponds to LASSO
    lambda_search = TRUE, # Search for lambda
  )
  
  # Getting coefficients
  result <- h2o.coef(fit)
  
  # Saving coefficients to csv
  write.csv(result, file.path(outdir, paste0(reg, '_coefs.csv')))
  print(paste0("Coefficients saved for ", reg, " regression!"))
}
