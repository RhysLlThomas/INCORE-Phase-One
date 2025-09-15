library(readr)
library(haven)
library(arrow)
library(tidyverse)
library(data.table)
library(Matrix)

#############################
##### 01_transform_data #####
#############################

read_data <- function(filepath) {
  
  # Getting file extension
  ext <- tools::file_ext(filepath)
  
  # Reading in data, using different methods depending on file extension
  if (ext == "csv") {
    df <- readr::read_csv(filepath)
  } else if (ext == "parquet") {
    df <- arrow::read_parquet(filepath)
  } else if (ext == "dta") {
    df <- haven::read_dta(filepath)
  } else if (ext == "sav") {
    df <- haven::read_sav(filepath)
  } else if (ext == "sas7bdat") {
    df <- haven::read_sas(filepath)
  } else if (ext %in% c("xls", "xlsx")) {
    df <- readxl::read_excel(filepath)
  } else if (ext == "rds") {
    df <- readRDS(filepath)
  } else {
    stop(paste0("Unsupported file type:", ext))
  }
  
  # Converting data to tibble
  df <- df %>%
    as_tibble()
  
  return (df)
}

subset_cols <- function(df, select_cols){
  
  # Getting columns to search
  col_names <- colnames(df)
  
  # If column name is exact match, treat is as an exact name
  # Otherwise, treat it as a regular expression
  cols <- c()
  for (col in select_cols) {
    if (col %in% col_names) {
      cols <- append(cols, col)
    } else {
      cols <- append(cols, str_subset(col_names, col))
    }
  }
  
  # Selecting subset columns
  df <- df %>%
    select(all_of(unique(cols)))
  
  return (df)
}

get_admission_id <- function(df, admission_id_cols=NULL){
  
  # If admission_id_cols are provided...
  if (!is.null(admission_id_cols)) {
    
    message("Using ",  paste(admission_id_cols, collapse = ", "), " columns to get year...")
    
    # Create an integer id for each unique grouping of the admission_id_cols
    df <- df %>%
      group_by(across(all_of(admission_id_cols))) %>%
      mutate(admission_id = cur_group_id()) %>%
      ungroup()
    
  } else {
    
    message("No admission_id_cols provided. Assigning unique interger as admission ID for each row.")
    
    # If no admission_id_cols are provided, use an integer as admission_id for each row in the data
    df <- df %>%
      mutate(admission_id = row_number())
  }
  return (df)
}

get_year_id <- function(df, year_col=NULL, discharge_date_col=NULL){
  
  # Checking that either year_col or discharge_date_colar supplied
  if (is.null(year_col) & is.null(discharge_date_col)){
    stop("Missing required columns. You must provided either a single length of stay column, or discharge date and admission date columns.")
  } else if (!is.null(year_col)) {
    
    # Converting year_col to a symbol
    year_col = rlang::sym(year_col)
    
    # Creating copy of year column
    message("Using ",  year_col, " column to get year...")
    df <- df %>%
      mutate(year_id = !!year_col)
    
  } else if (!is.null(discharge_date_col)) {
    
    # Converting discharge_date_col to symbol
    discharge_date_col = rlang::sym(discharge_date_col)
    
    
    # Creating new year_id column, using discharge date year
    message("Using ",  discharge_date_col, " columns to get year...")
    df <- df %>%
      mutate(year_id = year(!!discharge_date_col))
    
  }
  
  # Returning dataframe with new year_id column
  return(df)
}

get_age_bins <- function(df, age_col=NULL, birth_date_col=NULL, admission_date_col=NULL){
  
  # Checking that either age_col or birth_date_col and admission_date_col are supplied
  if (is.null(age_col) & (is.null(birth_date_col) & is.null(admission_date_col))){
    stop("Missing required columns. You must provided either a single age column, or birth date and admission date columns.")
  } else if (!is.null(age_col)) {
    
    # Converting age_col to a symbol
    age_col = rlang::sym(age_col)
    
    # Creating copy of age column to bin
    message("Using ",  age_col, " column to get age bins...")
    df <- df %>%
      mutate(age = !!age_col)
    
  } else if (!is.null(birth_date_col) & !is.null(admission_date_col)) {
    
    # Converting birth_date_col and admission_date_col to symbols
    birth_date_col = rlang::sym(birth_date_col)
    admission_date_col = rlang::sym(admission_date_col)
    
    
    # Creating new age_start column using bins
    message("Using ",  birth_date_col, " and ", admission_date_col, " columns to get age bins...")
    df <- df %>%
      mutate(age = time_length(difftime(!!admission_date_col, !!birth_date_col), "years"))
    
  }
  
  # Reading in age bins
  age_bins = read_feather(file.path("maps", "age_groups.feather"))
  
  # Creating a new column, corresponding to the starting age for each bin
  df <- df %>%
    mutate(age_start = case_when(age < 0 ~ -1,
                                 age == 0 ~ 0,
                                 TRUE ~ as.numeric(as.character(cut(
                                   age,
                                   breaks = c(age_bins[1,]$age_start, age_bins[-1,]$age_end),
                                   labels = age_bins[-1,]$age_start))))) 
  
  # Returning dataframe with new age_start column
  return(df)
}

get_sexes <- function(df, sex_col, sex_col_map){
  
  # Converting sex_col to a symbol
  sex_col = rlang::sym(sex_col)
  
  # Checking if all values in sex_col are included in sex_col_map, printing warning if not
  if (all(unique(df[[sex_col]]) %in% names(sex_col_map))) {
    invisible()
  }
  else {
    warning("Values found in ",  sex_col, " that do not exist in sex_col_map. These will be set to -1.")
  }
  
  # Creating sex_id column by mapping sex_col values
  sex_col_map = c(sex_col_map, list(".default"="-1"))
  df <- df %>% mutate(sex_id=recode(!!sex_col,
                                    !!!(sex_col_map)))
  
  # Returning dataframe with new sex_id column
  return(df)
}

get_icd_version <- function(df, icd_ver_col=NULL, icd_ver_col_map=NULL, discharge_date_col=NULL){
  
  # Checking if either discharge_date_col or icd_ver_col and icd_ver_col_map are supplied
  if (is.null(discharge_date_col) & (is.null(icd_ver_col) & is.null(icd_ver_col_map))){
    warning("No ICD version mapping columns or discharge date columns provided. Defaulting to using year_id.")
  }
  
  if (!is.null(icd_ver_col) & !is.null(icd_ver_col_map)) {
    
    # Converting icd_ver_col to a symbol
    icd_ver_col = rlang::sym(icd_ver_col)
    
    # Checking if all values in icd_ver_col are included in icd_ver_col_map, printing warning if not
    if (all(unique(df[[icd_ver_col]]) %in% names(icd_ver_col_map))) {
      invisible()
    }
    else {
      warning("Values found in ",  icd_ver_col, " that do not exist in icd_ver_col_map. These will be set to NULL.")
    }
    
    # Creating icd_ver_1 column by mapping icd_ver_col values
    icd_ver_col_map = c(icd_ver_col_map, list(".default"=NULL))
    df <- df %>% mutate(icd_ver_1=recode(!!icd_ver_col,
                                      !!!(icd_ver_col_map)))
  } else {
    
    # Setting value to NA if not provided
    df <- df %>% mutate(icd_ver_1=NA)
  }
  
  if (!is.null(discharge_date_col)) {
    
    # Converting discharge_date_col to symbol
    discharge_date_col = rlang::sym(discharge_date_col)
    
    # If discharge date is after 1st October, 2015 then assume ICD 10, otherwise assume ICD 9
    df <- df %>%
      mutate(icd_ver_2 = ifelse((difftime(!!discharge_date_col, "2015-10-1", units="days")>0), "icd10", "icd9"))
    
  } else {
    
    # Setting value to NA if not provided
    df <- df %>% mutate(icd_ver_2=NA)
  }
    
  # If year_id is > 2015 then assume ICD 10, otherwise assume ICD 9
  df <- df %>%
    mutate(icd_ver_3 = ifelse(year_id>2015, "icd10", "icd9"))
  
  # Coalescing columns encoded ICD version > discharge date derived ICD version > year_id derived ICD version 
  # Dropping icd_ver_1, icd_ver_2, and icd_ver_3 columns
  df <- df %>%
    mutate(icd_ver = coalesce(icd_ver_1, icd_ver_2, icd_ver_3)) %>%
    select(-c(icd_ver_1, icd_ver_2, icd_ver_3))
  
  return(df)
  
}

get_length_of_stay <- function(df, los_col=NULL, discharge_date_col=NULL, admission_date_col=NULL){
  
  # Checking that either los_col or discharge_date_col and admission_date_col are supplied
  if (is.null(los_col) & (is.null(discharge_date_col) & is.null(admission_date_col))){
    stop("Missing required columns. You must provided either a single length of stay column, or discharge date and admission date columns.")
  } else if (!is.null(los_col)) {
    
    # Converting los_col to a symbol
    los_col = rlang::sym(los_col)
    
    # Creating copy of length of stay column
    message("Using ",  los_col, " column to get length of stay...")
    df <- df %>%
      mutate(los = !!los_col)
    
  } else if (!is.null(discharge_date_col) & !is.null(admission_date_col)) {
    
    # Converting discharge_date_col and admission_date_col to symbols
    discharge_date_col = rlang::sym(discharge_date_col)
    admission_date_col = rlang::sym(admission_date_col)
    
    
    # Creating new length of stay column, in days, using date difference
    # Setting any negative values to -1
    message("Using ",  discharge_date_col, " and ", admission_date_col, " columns to get length of stay...")
    df <- df %>%
      mutate(los = as.integer(difftime(!!discharge_date_col, !!admission_date_col, units="days")),
             los = ifelse(los < 0, -1, los))
  }
  
  # Returning dataframe with new los column
  return(df)
}

get_conditions <- function(df, icd_cols, icd_condition_map){
  
  # Select ICD columns, allowing for regex
  cols = c()
  for (col in icd_cols) {
    cols <- append(cols, str_subset(colnames(df), col))
  }
  
  # Setting names with new standard name  (icd_#)
  cols <- setNames(cols, paste0("icd_", seq_along(cols)))
  
  # Pivot df to be long on condition columns, which are first renamed
  # Ensuring icd_level is treated as a factor
  df <- df %>%
    rename(!!!cols) %>%
    pivot_longer(cols = all_of(names(cols)), names_to = "icd_level", values_to = "icd_code") %>%
    drop_na(icd_code, icd_level) %>%
    mutate(icd_level = factor(icd_level, levels = names(cols)))
  
  # Join ICD map onto dataframe, using ICD map version and ICD code
  # Dropping anything that does not map in our ICD map
  df <- df %>%
    left_join(icd_condition_map, by = c("icd_ver"="icd_ver", "icd_code"="icd_code")) %>%
    drop_na(condition)
  
  return (df)
}

#########################
##### 02_clean_data #####
#########################

get_primary_condition <- function(df, NEC_other_lookup){
  
  # Assigning primary condition based on the following rules:
  # While looping over conditions for a single admission...
  # 1) Assign a condition as primary (starting with icd_level of icd_1)
  # 2) If the condition is _gc (garbage code), move to the next condition and
  # start over
  # 3) If the condition is exp_well_ (wellness condition) or rf_ (risk factor)
  # check if there are any other "valid" conditions and use the first one found,
  # otherwise use the current exp_well_ or rf_ condition
  # 4) If the condition is  _NEC (not elsewhere classified), check if there are
  # any other "valid" conditions within the same family and use the first one
  # found, otherwise use the current _NEC condition
  
  # Converting to data.table and setting order for better efficiency
  DT <- as.data.table(df)
  setorder(DT, admission_id, icd_level)
  
  # Adding flags for different conditions
  DT[, `:=`(
    condition_flag = fifelse(
      condition == "_gc", "gc",
      fifelse(
        startsWith(condition, "rf_") | startsWith(condition, "exp_well_"), "rf_exp",
        fifelse(endsWith(condition, "_NEC"), "NEC", "normal")
      )
    ),
    row_num = seq_len(.N)
  ), by = admission_id]
  
  # Applying primary assignment rules
  DT[, is_primary := {
    current_level <- 1L
    repeat {
      current_row <- .SD[current_level]
      flag <- current_row$condition_flag
      
      if (flag == "gc") {
        if (current_level < .N) {
          current_level <- current_level + 1L
        } else break
      } else if (flag == "rf_exp") {
        later <- .SD[row_num > current_row$row_num & condition_flag == "normal"]
        if (nrow(later)) {
          current_level <- later$row_num[1]
        } else break
      } else if (flag == "NEC") {
        fam <- NEC_other_lookup[[current_row$condition]]$family
        good_codes <- NEC_other_lookup[[fam]]$non_NEC_or_other_condition
        fam_match <- .SD[row_num > current_row$row_num & condition %in% good_codes]
        if (nrow(fam_match)) {
          current_level <- fam_match$row_num[1]
        } else break
      } else break
    }
    as.integer(row_num == current_level)
  }, by = admission_id]
  
  # Converting back to tibble and removing uneeded column
  df <- as_tibble(DT) %>%
    select(-c("condition_flag", "row_num"))
  
  return(df)
}

save_primary_counts <- function(df, year, age, sex, condition_families){
  
  # Getting primary condition row counts by year/age/sex/family/condition
  primary_counts <- df %>%
    filter(is_primary==1 &
             condition!="_gc" &
             !endsWith(condition, "_NEC")
           ) %>%
    left_join(condition_families,
              by="condition") %>%
    count(age_start,sex_id, year_id, family, condition, name = "n") %>%
    group_by(age_start, sex_id, year_id, family) %>%
    mutate(prop = n / sum(n)) %>%
    ungroup()
  
  # Saving out counts, partitioned by year/age/sex
  primary_counts %>%
    group_by(year_id, age_start, sex_id) %>%
    write_dataset(file.path("maps", "primary_condition_proportions.parquet"),
                  basename_template=paste0(year,"_",age,"_",sex,"_","_{{i}}.parquet"))
  
  return(primary_counts)
}

redistribute_conditions <- function(df, primary_counts, condition_families){
  
  # Summarizing primary count proportions to be family specific
  fam_pc <- primary_counts %>%
    group_by(family, condition) %>%
    summarize(n=sum(n)) %>%
    group_by(family) %>%
    mutate(tot_n=sum(n)) %>%
    ungroup() %>%
    mutate(prop=n/tot_n)
  
  # Finding all rows in dataframe with NEC or _gc condition
  df <- df %>%
    mutate(
      NEC_gc_flag = case_when(
        endsWith(condition, "_NEC") ~ "NEC",
        condition=="_gc" ~ "gc"
      )
    )
  
  # Reassigning NEC conditions to family-specific condition
  # Using family-specific proportions for random reassignment
  nec_rows <- df %>%
    filter(NEC_gc_flag=="NEC") %>%
    left_join(condition_families, by=c('condition')) %>%
    left_join(fam_pc, by = c("family"), relationship="many-to-many") %>%
    drop_na(condition.y, prop) %>%
    group_by(admission_id, icd_level) %>%
    summarize(
      condition = sample(condition.y, 1, prob = prop) 
    ) %>%
    ungroup()
  
  # Reassigning _gc conditions to any condition
  # Using age/sex/year-specific proportions for random reassignment
  gc_rows <- df %>%
    filter(NEC_gc_flag=="gc") %>%
    left_join(primary_counts, by = c("age_start", "sex_id", "year_id"), relationship="many-to-many") %>%
    group_by(admission_id, icd_level) %>%
    summarize(
      condition = sample(condition.y, 1, prob = prop) 
    ) %>%
    ungroup()
  
  # Updating data with reassigned conditions
  df <- df %>%
    rows_update(nec_rows, by=c("admission_id", "icd_level")) %>%
    rows_update(gc_rows, by=c("admission_id", "icd_level")) %>%
    left_join(condition_families, by = c("condition")) %>%
    select(-c(NEC_gc_flag))
  
  return(df)
}

##########################
##### 03_prep_inputs #####
##########################


create_reg_matrices <- function(DT, years, ages, conditions, families) {
  
  # Creating a datatable that's unique for each admission_id and contains age/year/los
  DT_admissions <- DT[, .SD[1], by = admission_id, .SDcols = c("age_start", "year_id", "los")]
  unique_admissions = unique(DT$admission_id)
  
  # Converting age_start and year_id to factors/dummies
  DT_admissions[, age_start := factor(age_start, levels = ages)]
  DT_admissions[, year_id := factor(year_id, levels = years)]

  # Creating datatable that's unique on admission_id with age_start encoded as
  # a dummy variable
  DT_age <- as.data.table(cbind(
    admission_id = DT_admissions$admission_id,
    model.matrix(~ age_start - 1, data = DT_admissions)
    ))
  
  # Creating datatable that's unique on admission_id with year_id encoded as
  # a dummy variable
  DT_year <- as.data.table(cbind(
    admission_id = DT_admissions$admission_id,
    model.matrix(~ year_id - 1, data = DT_admissions)
  ))
  
  # Cross-joining all conditions and admissions IDs
  full_grid_cond <- CJ(admission_id = unique_admissions, condition = conditions)
  
  # Finding all observed combinations of condition and admission_id
  observed_pairs <- unique(DT[, .(admission_id, condition)])
  
  # Marking pairs of condition and admission_id that were observed
  full_grid_cond[, present := 0L]
  setkey(full_grid_cond, admission_id, condition)
  setkey(observed_pairs, admission_id, condition)
  full_grid_cond[observed_pairs, present := 1L]
  
  # Pivoting from long to wide on condition, creating a datatable that's unique
  # on admission_id and contains a column for each condition. Contains whether 
  # or not a condition was seen for a given admission_id (1 or 0)
  DT_condition <- dcast(
    full_grid_cond,
    admission_id ~ condition,
    value.var = "present",
    fill = 0L
  )
  
  # Cross-joining all condition families and admissions IDs
  full_grid_family <- CJ(admission_id = unique_admissions, family = families)
  
  # Finding all observed combinations of condition and admission_id
  observed_fam_pairs <- unique(DT[, .(admission_id, family)])
  
  # Marking pairs of condition family and admission_id that were observed
  full_grid_family[, present := 0L]
  setkey(full_grid_family, admission_id, family)
  setkey(observed_fam_pairs, admission_id, family)
  full_grid_family[observed_fam_pairs, present := 1L]
  
  # Pivoting from long to wide on condition, creating a datatable that's unique
  # on admission_id and contains a column for each condition family. Contains 
  # whether or not a condition family was seen for a given admission_id (1 or 0)
  DT_family <- dcast(
    full_grid_family,
    admission_id ~ family,
    value.var = "present",
    fill = 0L
  )
  
  # Sorting by admission_id to ensure everything is aligned
  setkey(DT_admissions, admission_id)
  setkey(DT_condition, admission_id)
  setkey(DT_family, admission_id)
  setkey(DT_age, admission_id)
  setkey(DT_year, admission_id)
  
  # Converting each encoded datatable to a sparse matrix
  condition_mat <- as(Matrix(as.matrix(DT_condition[, -1, with=FALSE]), sparse=TRUE), "dgCMatrix")
  family_mat  <- as(Matrix(as.matrix(DT_family[, -1, with=FALSE]), sparse=TRUE), "dgCMatrix")
  age_mat  <- as(Matrix(as.matrix(DT_age[, -1, with=FALSE]), sparse=TRUE), "dgCMatrix")
  year_mat <- as(Matrix(as.matrix(DT_year[, -1, with=FALSE]), sparse=TRUE), "dgCMatrix")
  
  # Creating condition-age interaction matrix
  family_age_list <- list()
  family_age_names <- character()
  idx <- 1
  for (k in 1:ncol(age_mat)) {
    for (j in 1:ncol(family_mat)) {
      family_age_list[[idx]] <- family_mat[, j] * age_mat[, k]
      family_age_names[idx] <- paste0(colnames(age_mat)[k], "__", colnames(family_mat)[j])
      idx <- idx + 1
    }
  }
  family_age_mat <- do.call(cbind, family_age_list)
  colnames(family_age_mat) <- family_age_names
  
  # Creating family-pair interaction matrix
  family_pair_list <- list()
  family_pair_names <- character()
  idx <- 1
  for (i in 1:(ncol(family_mat) - 1)) {
    for (j in (i + 1):ncol(family_mat)) {
      family_pair_list[[idx]] <- family_mat[, i] * family_mat[, j]
      family_pair_names[idx] <- paste0(colnames(family_mat)[i], "__", colnames(family_mat)[j])
      idx <- idx + 1
    }
  }
  family_pair_mat <- do.call(cbind, family_pair_list)
  colnames(family_pair_mat) <- family_pair_names
  
  # Creating regression matrix for each equation and adding to list
  reg_matrices <- list(
    age_eq = cbind(los = DT_admissions$los, age_mat, year_mat),
    condition_eq = cbind(los = DT_admissions$los, condition_mat, year_mat),
    family_age_eq = cbind(los = DT_admissions$los, family_mat, age_mat, family_age_mat, year_mat),
    family_pair_eq = cbind(los = DT_admissions$los, family_mat, family_pair_mat, year_mat)
  )
  
  return (reg_matrices)
}

chunked_save <- function(sparse_mat, out_file, chunk_size = 100000) {
  
  # Finding number of rows to iterate over
  n_rows <- nrow(sparse_mat)
  
  # Initializing row counter
  row <- 1
  
  while (row <= n_rows) {
    
    # Get the row number to end at
    chunk_rows <- min(row + chunk_size - 1, n_rows)
    
    # Extract chunk of rows, starting with row and ending with chunk_rows
    chunk_mat <- sparse_mat[row:chunk_rows, , drop = FALSE]
    
    # Convert to data.frame format (i.e. a dense matrix)
    # This data.frame can be fairly large, hence the need for chunking
    chunk_DT <- as.data.frame(as.matrix(chunk_mat))
    
    # Saving to parquet format
    write_parquet(chunk_DT, sub("\\.parquet$", paste0("_chunk", row, ".parquet"), out_file))
    
    # Adding to row counter
    row <- chunk_rows + 1
  }
}