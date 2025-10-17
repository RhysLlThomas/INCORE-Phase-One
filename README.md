# International Network on COmparative REsource use (INCORE)

## Running the Code

Running the code for the INCORE project can be accomplished through the steps listed below. Before running any code

1. **Unzip and open the R project**
   - Open the `.Rproj` file in RStudio (or set the working directory to the `INCODE` folder in your R session)

2. **Install `renv`**
   - If not already installed, run ```install.packages("renv")```

3. **Restore the R environment**
   -  Run ```renv::restore()``` to restore all libraries used in this project

4. **Read through this README to understand the data, code, etc.**

5. **Modify `01_transform_data.R` as needed**
   - See instructions in the Source Code section below

6. **Run each step in order:**
   - `01_transform_data.R`
   - `02_clean_data.R`
   - `03_prep_inputs.R`
   - `04_run_regressions.R`

## Project Structure

This project is roughly structured in the following format:

  - `data/` > directory for data
    - `01_transformed_data` > folder that is written to by `01_transform_data.R`
    - `02_cleaned_data` > folder that is written to by `02_clean_data.R`
    - `03_prepped_inputs` > folder that is written to by `03_prep_inputs.R`
  - `src/` > source code directory
    - `01_transform_data.R` > transforms data to a standardized format
    - `02_clean_data.R` > runs some more complex data cleaning processes
    - `03_prep_inputs.R` > prepares data to be input to regression modes
    - `04_run_regressions.R` > runs regression models
    - `utils.R` > contains various functions used in other steps, for cleanliness 
  - `maps/` > directory for various maps
    - `age_groups.feather` > contains the binned age groups used in this project
    - `icd_map.feather` > contains a map from ICD codes (version 9 and 10) to conditions used in this project
    - `condition_details.feather` > contains details about conditions (name, aggregate conditions, etc.)
    - `NEC_other_conditions_lookup.feather` > contains details about redistributing "NEC" (not elsewhere classified) and "other" conditions, only needed for `02_clean_data`
  - `results/` > directory for model results/figures/etc.
  - `renv/` > directory for R libraries used in the project
  - `renv.lock` > lockfile that contains information about the R libaries used in this project's code
  - `README.md` > you are here
  - `INCORE.Rproj` > R Project options file

## Data

Data used in this project varies between teams. Generally, we expect to use admission level data, where **each row in a dataset corresponds to a single visit**. If you wish to use data that is less aggregated than this, you **will** need to do aggregation before running the code here. We expect each row in the data to contain the following columns:

- Year of data
- Unique patient identifier
- Patient age
    - OR patient date of birth and date of admission
- Patient sex
- ICD version used
    - OR date of discharge
- Diagnoses coded using the above ICD version
- Length of stay

Missing any of these variables in the data is okay, but some modification to the code may be required. For example, if the year of data is identfied in the name of a file, but does not appear as a column in the data, it should be added as a column in the `01_transform_data.R` step.

### 01_transformed_data

The data contained in this folder is written out by the `01_transform_data.R` script. This data contains standardized column names, pivots each row to be long on diagnoses/ICD codes (i.e. each admission will have 1 row per ICD code), and maps ICD codes to standard conditions.

This data also uses the "parquet" file format, which is useful for larger datasets. The parquet file format is used throughout this project, and can be read using most programming languages. Parquet files are compressed by default, so **do NOT** try to open files directly (i.e. don't click on them, it can cause weird issues).

### 02_cleaned_data

The data contained in this folder is written out by the `02_clean_data.R` script. This data is further cleaned, including a column to indicate a "primary" condition for each admission and redistributing non-specific conditions to specific ones.

### 03_prepped_inputs

The data contained in this folder is written out by the `03_prep_inputs.R` script. This data is formatted as a design matrix used for the regression models run in `04_run_regressions.R`.

## Source Code

The code used in this project is all based in the R statistical programming language. The libraries necessary for this project are included... somewhere

These scripts should be run in sequential order, starting with `01_transform_data.R` and ending with `04_run_regressions.R`. Most of these scripts are intended to be run without any modification, with the exception of `01_transform_data.R`, due to differences in raw input data.

Each script contains section titles and comments to help explain various processes. While they are not comprehensive documentation, they may be useful for troubleshooting bugs and general code readability. Section titles appear as:

~~~
#------------------------
##### SECTION TITLE #####
#------------------------
~~~

An important note about these scripts is that they are NOT parallelized. Due to potential differences in available technology, these scripts are written to run with minimal computational resources (though not using those resources particularly efficiently). This means that ***THIS CODE CAN BE VERY SLOW AT TIMES***. However, since most of these scripts are embarrassingly parallel, they can be easily adapted as needed if desired.

### 01_transform_data.R

This script reads in raw input data, creates standardized columns, maps ICD codes to conditions, and saves the resulting data to a parquet file within the  `01_transformed_data` directory.

***This script will require modification before use!***

Because the sources of data used in the project are varied, this script will need to be altered to ensure that it works correctly. There are several helper functions in the `utils.R` file that this script uses, which are designed to be flexible for different data sources. A more complete list of those functions can be found in the `utils.R` section.

There are several important factors that need to be considered before running this code:

1. Is your data aggregated to the admission/visit level?

We expect that a single row in the raw input data corresponds to a single admission/visit. Less aggregated data will not fail at this step, but it **will** cause issues down the line and any results will be misleading.

2. What is the format of your data?

This code has a function to load raw data files that are in the any of the following formats: `.csv`, `.parquet`, `.dta`, `.sav`, `.sas7bdat`, `.xlsx`, `.xls`, or `.rds`.If your data is in a different format, you will need to modify how this script reads in data. It may also be helpful to parallelize this step, if able.

3. What are the columns and column names available in your data?

Data sources will have varying names and columns, requiring some manual changes to how the standardized columns are created. There is a section at the top of this script titled "USER INPUTS." The code in this section should be modified for your specific datasource. Namely, there are several variables that need to be assigned in order to ensure processing works as intended. Any variables that are not present in the data can be made "NULL", but there are some variables that are required. Consult the script comments for specific details about each variable.

This script will write out a partitioned parquet dataset to the `data/01_transformed_data` folder. Besides standardizing the data, this step will also transform the data to be long on ICD code. This means that if a single admission/row had 5 ICD codes, it will end up being 5 rows in the transformed data. Because of this, the transformed data can end up being very large and take up significant disk space. Future steps are broken up into smaller pieces to account for the increase in number of rows, but you still may encounter issues with data size.

### 02_clean_data.R

This script reads in transformed data, assigns a primary condition to each admission_id, redistributes non-specific conditions, and saves the resulting data to a parquet file within the `02_cleaned_data` directory. This script **should NOT** require any modification, since the input data should be standardized.

The first step of `02_clean_data.R` is to assign a condition as the primary condition for a given admission. This is done via the following steps.

1. Flag all non-specific conditions
    - Non-specific conditions include `_gc` (garbage codes), `rf_*` (risk factor codes), `exp_well_*` (wellness check up codes), or `*_NEC` (not elsewhere classified codes).
    - Specific conditions are anything else

2. Group by admission_id and create a primary indicator, assigning the first condition (the condition of the first ICD code) as primary

3. Loop over all conditions for a given admission_id and apply the following logic:
    1. If the condition is a specific condition, mark it as primary
    2. Check if the primary condition is `_gc`. If it is, mark the next condition available as primary and repeat these steps
    3. If the condition is `rf*` or `exp_well_*`, check if there are any other specific conditions and mark the first one found as primary. Otherwise, mark the current `exp_well_*` or `rf_*` condition as primary
    4. If the condition is `*_NEC`, check if there are any specific conditions within the same condition family. If there are, mark the first one found as primary. Otherwise, mark the current `*_NEC` condition as primary
    
The second step is to redistribute any remaining non-specific conditions to specific conditions. In this step, only `_gc` and `*_NEC` conditions are redistributed. This process works by first finding the proportion of conditions assigned as primary (written out to `maps/primary_condition_proportions.parquet`). These proportions are then applied to resdistribute the remaining non-specific conditions. The redistribution process always uses year-age-sex specific proportions.

- For `*_NEC` conditions, redistribution is based on condition family-specific proportions. That is, `*_NEC` conditions can only be redistributed to conditions within the same family.
  - If family-specific proportions are not available, then the same logic used to redistribute `_gc` is applied. This means that `*_NEC` conditions can sometimes map to non-family-specific conditions.
- For `_gc`, resdistribution is across all possible conditions. That is, `_gc` can be redistributed to any condition.

This script will write out a partitioned parquet dataset to the `data/02_cleaned_data` folder.  As before, this data is long on condition, so it can be fairly large.

### 03_prep_inputs.R

***This script MAY require modification before use!***

This script reads in cleaned data, creates a set of regression matrices, and saves the resulting data to a set of directories within the  `03_prepped_inputs` directory. This script **MAY** require modification, depending on the regressions being run. In particular, if person-year level regression matrices need to be created, then the `reg_level` variable should be set to `"person_year"`. This variable defaults to "admissions", which will create admission level regression matrices.

Creating regression matrices is done using the `data.table` and `Matrix` libraries due to more efficient memory usage. Namely, `data.table` allows for a more efficient pivoting of the data from being long on condition to wide. Similarly, due to the sparsity of the regression matrices, `Matrix` allows the use of a more compressed regression matrix format.

All right-hand-side (RHS) variables are one-hot encoded in this script (1 if it exists for an admission, otherwise 0). This means that the matrix for each regression equation will have a column for each possible value of all variables. For the last two analyses of the INCORE project (the equations containing age-condition and condition-pair interactions), there are ~400 columns to represent all encoded variables due to the large number of interactions (17 age groups * 31 condition family interaction terms and 31(31-1)/2 pairwise condition family interaction terms).

It should be noted that all regressions matrices are currently made in a single function. This is computationally faster, but may result in memory issues if the data size is too large.

This script will write out a parquet dataset to the `data/03_prepped_inputs` folder. This dataset is NOT partitioned, contains a single row for each admission, and is encoded for use in the regression analysis

### 04_run_regressions.R

***This script MAY require modification before use!***

Finally, this script runs regressions on the data from `data/03_prepped_inputs` and outputs the results. Regressions are run using the `h2o` library, which, again, is used to assist with memory management. This script **MAY** require modification, depending on the regressions being run. In particular, if running person-year level regressions is desired, then the `reg_level` variable should be set to `"person_year"`. This variable defaults to "admissions", which will run admission level regressions.

The `h2o` library has to load the data before it can run a regression, which can take a bit of time. Fitting the regression model itself is fairly fast, however.

This code runs 2 regressions per regression equation, the first is a LASSO regression used for variable selection and the second is a standard GLM regression used for determining coefficients and standard errors. The coefficients and fit statistics (MSE, RMSE, R^2, and AIC) from both regressions are saved in the `results` folder. A prefix/suffix of `LASSO_/_LASSO` corresponds to results from the LASSO regression model while a prefix/suffix of `GLM_/_GLM` corresponds to results from the GLM regression model.

### utils.R

Most of the working code is written in the file, to help keep the primary scripts clean and simple. This file is sourced by each of the primary scripts and contains a number of functions used in each step. Ideally, this code should not need to be changed or adapted across teams.

## Maps

The maps folder contains a handful of key maps for use in this project.

- **age_groups.feather**
  - A feather file that contains information about how to bin ages into different groups, including starting age for group and full name of group
- **icd_map.feather**
  - A feather file that contains mappings from ICD codes to 169 different conditions. Contains mappings for both ICD 9 and ICD 10 versions
- **condition_details.feather**
  - A feather file that information about the 169 conditions, including full name of condition and condition family
- **NEC_other_conditions_lookup.feather**
  - A feather file that information about `*_NEC` conditions and their families, only used in `02_clean_data.R` to assist with the redistribution process
- **primary_condition_proportions.parquet**
  - A parquet file that contains the year/age/sex specific proportions of each condition assigned as primary. This file will not be present until `02_clean_data.R` is run
