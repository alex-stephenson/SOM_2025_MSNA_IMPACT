rm(list = ls())
### post collection checks

library(tidyverse)
library(cleaningtools)
library(readxl)

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  select(admin_2, fo_in_charge = FO_In_Charge)

## raw data
all_raw_data <- read_rds("03_output/01_raw_data/all_raw_data.rds")


## tool
kobo_tool_name <- "04_tool/REACH_2024_MSNA_kobo tool.xlsx"
questions <- read_excel(kobo_tool_name, sheet = "survey")
choices <- read_excel(kobo_tool_name, sheet = "choices")


# Read in all the clogs
file_list <- list.files(path = "01_cleaning_logs", recursive = TRUE, full.names = TRUE)

file_list <- file_list %>%
  keep(~ str_detect(.x, "May_09"))

# Function to read and convert all columns to character
read_and_clean <- function(file, sheet) {
  read_excel(file, sheet = sheet) %>%
    mutate(across(everything(), as.character)) %>%   # Convert all columns to character
    mutate(file_path = file)
}

# Read and combine all files into a single dataframe
cleaning_logs <- map_dfr(file_list, sheet = 'cleaning_log', read_and_clean)

clogs_split <- cleaning_logs %>%
  group_by(clog_type) %>%
  group_split() %>%
  set_names(map_chr(., ~ unique(.x$clog_type)))


common_rosters <- intersect(
  names(all_raw_data),
  cleaning_logs$clog_type %>% unique() %>% na.omit()
)

list_of_df_and_clog <- map(common_rosters, function(g) {
  raw <- all_raw_data[[g]] %>% mutate(across(everything(), as.character))
  clog <- clogs_split[[g]] %>% mutate(across(everything(), as.character))

  list(
    raw_data = raw,
    cleaning_log = clog
  )
})

# Read in all the dlogs
all_dlogs <- readxl::read_excel(r"(03_output/02_deletion_log/deletion_log.xlsx)", col_types = "text")
#manual_dlog <- readxl::read_excel("03_output/deletion_log_manual/DSRA_II_Manual_Deletion_Log.xlsx", col_types = "text")
cleaning_log_deletions <- cleaning_logs %>%
  filter(change_type == "remove_survey") %>%
  mutate(interview_duration = "") %>%
  select(uuid, site_name, enum_name,interview_duration, comment = issue)

all_deletions <- bind_rows(all_dlogs, manual_dlog) %>%
  bind_rows(cleaning_log_deletions)


### now find a way to apply to each cleaning log the others, remove the deletions and then apply the cleaning_logs

cleaning_log_summaries <- purrr::map(list_of_df_and_clog, function(x) {

  # Apply transformations step-by-step to ensure column types stay correct
  cleaned_log <- x$cleaning_log %>%
    filter(!index %in% deletion_log$index) %>%
    mutate(new_value = as.character(new_value))  # <- This is the key line

  updated_log <- update_others(questions, choices, cleaned_log)

  cleaned_data <- x$raw_data %>%
    filter(!index %in% deletion_log$index) %>%
    mutate(across(everything(), as.character))

  list(
    cleaning_log = updated_log,
    raw_data = cleaned_data
  )
})

### apply the cleaning log to each items in the list

clean_data_logs <- purrr::map(cleaning_log_summaries, function(x) {

  my_clean_data <- create_clean_data(raw_dataset = x$raw_data,
                                     raw_data_uuid_column = "uuid",
                                     cleaning_log = x$cleaning_log,
                                     cleaning_log_uuid_column = "uuid",
                                     cleaning_log_question_column = "question",
                                     cleaning_log_new_value_column = "new_value",
                                     cleaning_log_change_type_column = "change_type")

  my_clean_data_parentcol <- recreate_parent_column(dataset = my_clean_data,
                                                    uuid_column = "uuid",
                                                    kobo_survey = kobo_survey,
                                                    kobo_choices = kobo_choice,
                                                    sm_separator = "/",
                                                    cleaning_log_to_append = cleaning_logs_amended)

  list(

    cleaning_log <- my_clean_data_parentcol$cleaning_log,
    my_clean_data_final <- my_clean_data_parentcol$data_with_fix_concat

    )

})





############################################# soft duplicates #####################################################

## read in already approved ones
exclusions <- read_excel("02_input\02_duplicate_exclusions/exclusions.xlsx")

my_clean_data_added <- cleaning_log_final %>%
  left_join(fo_district_mapping)

enum_typos <- my_clean_data_added %>%
  dplyr::count(enum_name) %>%
  filter(n >= 5) %>%
  pull(enum_name)

group_by_enum <- my_clean_data_added %>%
  filter(enum_name %in% enum_typos) %>%
  group_by(enum_name)

soft_per_enum <- group_by_enum %>%
  dplyr::group_split() %>%
  purrr::map(~ check_soft_duplicates(dataset = .,
                                     kobo_survey = kobo_survey,
                                     uuid_column = "uuid",
                                     idnk_value = "dnk",
                                     sm_separator = "/",
                                     log_name = "soft_duplicate_log",
                                     threshold = 5
  )
  )

# recombine the similar survey data
similar_surveys <- soft_per_enum %>%
  purrr::map(~ .[["soft_duplicate_log"]]) %>%
  purrr::map2(
    .y = dplyr::group_keys(group_by_enum) %>% unlist(),
    ~ dplyr::mutate(.x, enum = .y)
  ) %>%
  do.call(dplyr::bind_rows, .)


similar_surveys_with_info <- similar_surveys %>%
  left_join(my_clean_data_added, by = "uuid") %>%
  left_join(my_clean_data_added %>% select(uuid, similiar_survey_date = today), by = join_by("id_most_similar_survey" == "uuid")) %>%
  select(district, idp_hc_code, fo, today, start, end, uuid, issue, enum_name, num_cols_not_NA, total_columns_compared, num_cols_dnk, similiar_survey_date, id_most_similar_survey, number_different_columns) %>%
  filter(! uuid %in% exclusions$uuid & ! id_most_similar_survey %in% exclusions$id_most_similar_survey,
         today == similiar_survey_date)


similar_survey_raw_data <- my_clean_data %>%
  filter(uuid %in% (similar_surveys_with_info$uuid))

similar_survey_export_path <- paste0("03_output/04_similar_survey_check/similar_surveys_", today(), ".xlsx")

# create a workbook with our data

similar_survey_output <- list("similar_surveys" = similar_surveys_with_info, "similar_survey_raw_data" = similar_survey_raw_data)

similar_survey_output %>%
  writexl::write_xlsx(., similar_survey_export_path)


