# ─────────────────────────────────────────────────────────────────────────────────
# This code uses the Somalia standardised approach for producing clogs, applying
# them and making raw, clean, clogs and dlogs (deletion logs) on a daily basis.
#
# Within the MSNA framework it is more complicated because we have to allow for
# multiple repeat sections within the data. This code extensively uses iterative
# code to achieve this, primarily from the {purrr} library.
#
# The code basically follows these steps
# ➡️ Read in  raw data via the Kobo API, see package {ImpactFunctions} from
# https://github.com/alex-stephenson/ImpactFunctions. The output is dm object,
# where each element in the object is either the main data or a part of the repeat.
# ➡️ The main df is filtered using the metadata duration, also calculated from
# {ImpactFunctions}, creating a deletion log in the process
# ➡️ Produces clogs for each element in the dm object, doing a standardised set of
# transformations in the process.
# Combining all the sets of clogs, and outputting one clog for every repeat, split
# by field officer.
# ─────────────────────────────────────────────────────────────────────────────────


rm(list = ls())

library(cleaningtools, exclude = c('create_xlsx_cleaning_log', 'create_validation_list')) ## we use an updated local version which has drop down for others. See utils
library(tidyverse)
library(readxl)
library(openxlsx)
library(ImpactFunctions)
library(robotoolbox)
library(impactR4PHU)


date_to_filter <- "2025-06-26"
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

source("00_src/00_utils.R")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load all the data
# ──────────────────────────────────────────────────────────────────────────────

geo_ref_data <- readxl::read_excel("02_input/07_geo_reference_data/sample_frame_SOM_MSNA_2025.xlsx", sheet = "sample_frame_SOM_MSNA_2025") %>%
  select(unit_of_analysis, district, hex_ID, survey_buffer)


geo_admin1 <- geo_ref_data %>%
  distinct(admin_1_pcode, admin_1_name)
geo_admin2 <- geo_ref_data %>%
  distinct(admin_2_pcode, admin_2_name)
geo_admin3 <- geo_ref_data %>%
  distinct(admin_3_pcode, admin_3_name)



kobo_tool_name <- "04_tool/REACH_SOM2503_MSNA_2025_IPC.xlsx" ## make sure this gets updated
questions <- read_excel(kobo_tool_name, sheet = "survey")
choices <- read_excel(kobo_tool_name, sheet = "choices")

data_file_path <- "02_input/00_data_download/REACH_SOM_2025_MSNA.xlsx"
sheet_names <- readxl::excel_sheets(data_file_path)
raw_kobo <- map(sheet_names, ~ read_excel(data_file_path, sheet = .x, guess_max =  10000))
sheet_names[1] <- "main"
names(raw_kobo) <- sheet_names

raw_kobo_data <- raw_kobo %>%
  pluck("main") %>%
  dplyr::rename(uuid =`_uuid`,
                index = `_index`) %>%
  mutate(across(ends_with("_other"), as.character))

uuid_names <- questions %>%
  filter(calculation == "uuid()") %>%
  pull(name)

uuid_names[-1]
names(raw_kobo)

roster_uuids <- tibble(
  name         = names(raw_kobo)[-1],
  uuids        = uuid_names[2:7],
  repeat_table = names(raw_kobo)[-1]
)

# Define a function that processes each element
process_roster <- function(name, uuids, repeat_table) {
  raw_kobo[[ name ]] %>%
    rename(
      index        = `_parent_index`,
      roster_index = `_index`
    ) %>%
    mutate(uuid = .data[[ uuids ]]) %>%
    distinct(uuid, .keep_all = TRUE)
}

roster_outputs <- pmap(
  roster_uuids,
  process_roster
)

names(roster_outputs) <- roster_uuids$name

roster_outputs[['main']] <- raw_kobo_data

roster_outputs %>%
  write_rds(., "03_output/01_raw_data/all_raw_data.rds")

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print("~~~~~~There are multiple versions of the tool in use~~~~~~")
}

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  rename(fo_in_charge = FO_In_Charge)

# # join the fo to the dataset
data_with_fo <- raw_kobo_data %>%
  left_join(fo_district_mapping %>% select(-admin_1_name), by = join_by("admin1" == "admin_1_pcode"))

data_with_fo <- data_with_fo %>%
  filter(!is.na(fo_in_charge))

# ──────────────────────────────────────────────────────────────────────────────
# 2. Filter for time and export deleted surveys
# ──────────────────────────────────────────────────────────────────────────────

mindur <- 19
maxdur <- 150


kobo_data_metadata <- get_kobo_metadata(dataset = data_with_fo, un = "alex_stephenson", asset_id = asset_id, remove_geo = T)
data_with_time <- kobo_data_metadata$df_and_duration
raw_metadata_length <- kobo_data_metadata$audit_files_length
write_rds(raw_metadata_length, "03_output/01_raw_data/raw_metadata.rds")


data_in_processing <- data_with_time %>%
  mutate(length_valid = case_when(
    interview_duration <= mindur ~ "Too short",
    interview_duration >= maxdur ~ "Too long",
    TRUE ~ "Okay"
  )) %>%
  mutate(length_valid = ifelse(interview_duration > 15 & interview_duration < 20 & hh_size < 5, "Okay", length_valid),
         length_valid = ifelse(interview_duration > 10 & interview_duration < 15 & hh_size < 2, "Okay", length_valid))

remove_deletions <- readxl::read_excel("02_input/08_remove_deletions/remove_deletions.xlsx")

deletion_log <- data_in_processing %>%
  filter(length_valid != "Okay") %>%
  select(uuid, length_valid, admin1, admin_2_camp, admin_3_camp, enum_id, interview_duration) %>%
  left_join(raw_kobo_data %>% select(uuid, index), by = "uuid") %>%
  filter(! uuid %in% remove_deletions$uuid)


deletion_log %>%
  mutate(comment = paste0("Interview length is ", length_valid)) %>%
  select(-length_valid) %>%
  writexl::write_xlsx(., paste0("03_output/02_deletion_log/deletion_log.xlsx"))

## filter only valid surveys and for the specific date
data_valid_date <- data_in_processing %>%
  mutate(length_valid = case_when(
    uuid %in% remove_deletions$uuid ~ "Okay",
    TRUE ~ length_valid
  )) %>%
  filter(length_valid == "Okay") %>%
  filter(today == max(today))

main_data <- data_valid_date %>%
  addindicators::add_eg_fcs(cutoffs = "normal")

# ──────────────────────────────────────────────────────────────────────────────
# 3. GIS Checks
# ──────────────────────────────────────────────────────────────────────────────

gps<-main_data %>% filter(consent=="yes") %>%
  select(uuid,enum_id,today, survey_modality, contains("camp"),contains("sub_camp"),contains("point_number"), contains("check_ptno_insamples"), contains("validate_ptno"), contains("pt_sample_lat"), contains("pt_sample_lon"), contains("dist_btn_sample_collected"), contains("reasons_why_far"), contains("geopoint"))
write.xlsx(gps , paste0("03_output/03_gps/gps_checks_", lubridate::today(), ".xlsx"))

# ──────────────────────────────────────────────────────────────────────────────
# 4. Apply the checks to the main data
# ──────────────────────────────────────────────────────────────────────────────

clog_metadata <- c("admin1", "admin_2_camp", "admin_3_camp", "today","enum_id", "fo_in_charge", "index")
exclude_patterns <- c("geopoint", "gps", "_index", "_submit", "submission", "_sample_", "^_id$", "^rand", "^_index$","_n","enum_id", "ind_potentially_hoh")

## calculate other questions
others_to_check <- questions %>%
  filter(type == "text",
         str_detect(name, "other")) %>%
  pull(name)

df_list_logical_checks <- read_csv("02_input/01_logical_checks/check_list.csv")

excluded_questions <- questions %>%
  filter(type != "integer" & type != "calculate") %>%
  pull(name) %>%
  unique()

outliers_exclude_main <- calc_outlier_exclusion(raw_kobo_data)

checked_main_data <-  main_data %>%
  run_standard_checks(value_check = TRUE,
                      outlier_check = TRUE,
                      columns_not_to_check_outliers = outliers_exclude_main,
                      other_check = TRUE,
                      columns_to_check_others = names(main_data %>% select(contains(others_to_check))),
                      logical_check = TRUE,
                      logical_list = df_list_logical_checks)

main_cleaning_log <-  checked_main_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = clog_metadata
  )

# ──────────────────────────────────────────────────────────────────────────────
# 5. Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

main_to_join <- main_data %>%
  dplyr::select(admin1, admin_2_camp, admin_3_camp, today,enum_id,resp_gender, enum_gender,
                hoh_gender,fo_in_charge,deviceid,instance_name, index)


trans_roster <- function(data) {
  data %>%
    dplyr::filter(!.data$index %in% deletion_log$index) %>%
    dplyr::filter(.data$index %in% main_data$index) %>%
    dplyr::left_join(main_to_join, by = join_by(index == index))
}

roster_outputs$main <- NULL
roster_outputs_trans <- map(roster_outputs, trans_roster)

purrr::walk2(roster_outputs_trans, roster_uuids$name, ~assign(.y, .x, envir = .GlobalEnv))

# ──────────────────────────────────────────────────────────────────────────────
#  5.1 HH Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

outliers_exclude_roster <- calc_outlier_exclusion(roster)

checked_hh_roster <- roster %>%
  run_standard_checks(
    value_check = TRUE,
    outlier_check = FALSE,
    other_check = TRUE,
    logical_check = FALSE,
    columns_to_check_others = outliers_exclude_roster)

hh_roster_cleaning_log <-  checked_hh_roster %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = clog_metadata
  )


# ──────────────────────────────────────────────────────────────────────────────
#  5.2 WGS data cleaning
# ──────────────────────────────────────────────────────────────────────────────

df_list_logical_wgs <- read_csv("02_input/01_logical_checks/check_list_wgs.csv")

checked_wgs <- wgs %>%
  run_standard_checks(
    value_check = FALSE,
    outlier_check = FALSE,
    other_check = FALSE,
    logical_check = TRUE,
    logical_list = df_list_logical_wgs)

checked_wgs_cleaning_log <- checked_wgs %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = clog_metadata
  )

# ──────────────────────────────────────────────────────────────────────────────
#  5.3 Child feeding data cleaning
# ──────────────────────────────────────────────────────────────────────────────

outliers_exclude_child_feeding <- calc_outlier_exclusion(child_feeding)


checked_child_feeding <- child_feeding %>%
  run_standard_checks(
    value_check = TRUE,
    outlier_check = TRUE,
    columns_not_to_check_outliers = c(outliers_exclude_child_feeding, "time_breastfeeding_hours"),
    other_check = TRUE,
    columns_to_check_others = names(child_feeding %>% select(contains(others_to_check))),
    logical_check = FALSE
)

child_feeding_cleaning_log <-  checked_child_feeding %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = clog_metadata
)

# ──────────────────────────────────────────────────────────────────────────────
#  5.4 Child nutrition Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

### no planned checks

# ──────────────────────────────────────────────────────────────────────────────
#  5.4 MUAC data cleaning
# ──────────────────────────────────────────────────────────────────────────────

## need to do outliers for MUAC, and maybe a logical check??


# ──────────────────────────────────────────────────────────────────────────────
#  5.4 Child vaccination data cleaning
# ──────────────────────────────────────────────────────────────────────────────

#### cant think of any checks

# ──────────────────────────────────────────────────────────────────────────────
#  Mortality data cleaning
# ──────────────────────────────────────────────────────────────────────────────

df_list_logical_mortality <- read_csv("02_input/01_logical_checks/check_list_wgs.csv")

outliers_exclude_mortality <- calc_outlier_exclusion(died_member)

died_member_checked <- died_member %>%
  run_standard_checks(
    value_check = TRUE,
    outlier_check = TRUE,
    columns_not_to_check_outliers = outliers_exclude_mortality,
    other_check = TRUE,
    columns_to_check_others = names(died_member %>% select(contains(others_to_check))),
    logical_check = TRUE,
    logical_list = df_list_logical_mortality,
  )

died_member_cleaning_log <-  died_member_checked %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    information_to_add = clog_metadata
  )

# ──────────────────────────────────────────────────────────────────────────────
#  6. Clog output
# ──────────────────────────────────────────────────────────────────────────────


final_clog <- bind_rows(
  main_cleaning_log$cleaning_log %>% mutate(clog_type = "main"),
  hh_roster_cleaning_log$cleaning_log %>% mutate(clog_type = "roster"),
  health_cleaning_log$cleaning_log %>% mutate(clog_type = "health_ind"),
  child_feeding_cleaning_log$cleaning_log %>% mutate(clog_type = "nut_ind"),
  edu_cleaning_log$cleaning_log %>% mutate(clog_type = "edu_ind"))


final_clog <- final_clog %>%
  filter(!is.na(fo_in_charge))

final_checked_data <- main_cleaning_log$checked_dataset %>%
  filter(!is.na(fo_in_charge))

# Get distinct group levels from both datasets
common_groups <- intersect(
  final_checked_data$fo_in_charge %>% unique() %>% na.omit(),
  final_clog$fo_in_charge %>% unique() %>% na.omit()
)

# Create named lists keyed by fo_in_charge
checked_split <- final_checked_data %>%
  filter(fo_in_charge %in% common_groups) %>%
  group_split(fo_in_charge, .keep = TRUE) %>%
  set_names(map_chr(., ~ unique(.x$fo_in_charge))) %>%
  keep(~ nrow(.x) > 0)

clog_split <- final_clog %>%
  filter(fo_in_charge %in% common_groups) %>%
  group_split(fo_in_charge, .keep = TRUE) %>%
  set_names(map_chr(., ~ unique(.x$fo_in_charge))) %>%
  keep(~ nrow(.x) > 0)

cleaning_log <- map(common_groups, function(g) {
  checked <- checked_split[[g]]
  clog <- clog_split[[g]]

  list(
    checked_dataset = checked,
    cleaning_log = clog
  )
})

options(openxlsx.na.string = "")
cleaning_log %>% purrr::map(~ create_xlsx_cleaning_log(.[],
                                                       cleaning_log_name = "cleaning_log",
                                                       change_type_col = "change_type",
                                                       column_for_color = "check_binding",
                                                       header_front_size = 10,
                                                       header_front_color = "#FFFFFF",
                                                       header_fill_color = "#ee5859",
                                                       header_front = "Calibri",
                                                       body_front = "Calibri",
                                                       body_front_size = 10,
                                                       use_dropdown = T,
                                                       use_others = T,
                                                       sm_dropdown_type = "numerical",
                                                       kobo_survey = questions,
                                                       kobo_choices = choices,
                                                       output_path = paste0("01_cleaning_logs/",
                                                                            unique(.[]$checked_dataset$fo_in_charge),
                                                                            "/",
                                                                            "final_cleaning_log_",
                                                                            unique(.[]$checked_dataset$fo_in_charge),
                                                                            "_",
                                                                            date_time_now,
                                                                            ".xlsx")))
