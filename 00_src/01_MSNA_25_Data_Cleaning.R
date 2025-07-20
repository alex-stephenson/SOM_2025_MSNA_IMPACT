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


date_to_filter <- "2025-07-17"
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

source("00_src/00_utils.R")

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load all the data
# ──────────────────────────────────────────────────────────────────────────────

geo_ref_data <- readxl::read_excel("02_input/07_geo_reference_data/sample_frame_SOM_MSNA_2025.xlsx", sheet = "sample_frame_SOM_MSNA_2025") %>%
  select(unit_of_analysis, district, admin_2, hex_ID, survey_buffer)

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  rename(fo_in_charge = FO_In_Charge)

point_data <- read_csv("04_tool/sample_points.csv")

site_data <- readxl::read_excel("02_input/05_site_data/Site_Master_List.xlsx")

kobo_tool_name <- "../../01_MSNA 2025_ Research Design/REACH_SOM2503_MSNA_2025_IPC.xlsx"
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


roster_uuids <- tibble(
  name         = names(raw_kobo)[-1],
  uuids        = uuid_names[-1],
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

roster_outputs[['wgs_repeat']] <- roster_outputs[['wgs_repeat']] %>%
  mutate(wgs_age = as.numeric(wgs_age)) %>%
  filter(wgs_age >= 5)

roster_outputs[['nut_ind']] <- roster_outputs[['nut_ind']] %>%
  mutate(nut_ind_age = as.numeric(nut_ind_age)) %>%
  filter(nut_ind_age < 5)

roster_outputs[['child_feeding']] <- roster_outputs[['child_feeding']] %>%
  mutate(child_age = as.numeric(child_age)) %>%
  mutate(child_age_months = as.numeric(child_age_months)) %>%
  filter(child_age < 2)

roster_outputs %>%
  write_rds(., "03_output/01_raw_data/all_raw_data.rds")

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print("~~~~~~There are multiple versions of the tool in use~~~~~~")
}

# # join the fo to the dataset
data_with_fo <- raw_kobo_data %>%
  left_join(fo_district_mapping)

if (sum(is.na(data_with_fo$fo_in_charge)) > 0) {
  stop("~~~~~~!!Join of FO not entirely successful!!~~~~~~")
}

data_in_processing <- data_with_fo

# ──────────────────────────────────────────────────────────────────────────────
# 2. Filter for time and export deleted surveys
# ──────────────────────────────────────────────────────────────────────────────

mindur <- 20
maxdur <- 150

asset_id = "aFUeVboh9TJtLAqDMwL8ov"
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
  select(uuid, length_valid, admin_1, admin_2, admin_3, enum_id, interview_duration) %>%
  left_join(raw_kobo_data %>% select(uuid, index), by = "uuid") %>%
  filter(! uuid %in% remove_deletions$uuid)

deletion_log %>%
  mutate(comment = paste0("Interview length is ", length_valid)) %>%
  select(-length_valid) %>%
  writexl::write_xlsx(., paste0("03_output/02_deletion_log/deletion_log.xlsx"))

data_in_processing %>%
  filter(length_valid != "Okay") %>%
  left_join(site_data %>% select(idp_hex_id = settlement_idp, Name)) %>%
  select(uuid, length_valid, admin_1_name, admin_2_name, admin_3, point_id, idp_hex_id, Name, enum_id, interview_duration) %>%
  writexl::write_xlsx(., paste0("03_output/02_deletion_log/detailed_deletions.xlsx"))


## filter only valid surveys and for the specific date
data_valid_date <- data_in_processing %>%
  mutate(length_valid = case_when(
    uuid %in% remove_deletions$uuid ~ "Okay",
    TRUE ~ length_valid
  )) %>%
  filter(length_valid == "Okay") %>%
  filter(as_date(today) == date_to_filter)

## add FCS calc for clogs
main_data <- data_valid_date %>%
  addindicators::add_eg_fcs(cutoffs = "normal")

# ──────────────────────────────────────────────────────────────────────────────
# 3. GIS Checks
# ──────────────────────────────────────────────────────────────────────────────

gps<-main_data %>%
  select(uuid,enum_id,today, access_location, contains("admin"), point_id, contains("gps"), distance_to_site, fo_in_charge) %>%
  left_join(point_data %>% select(point_id = Point_ID, Hex_ID))
write.xlsx(gps , paste0("03_output/03_gps/gps_checks_", lubridate::today(), ".xlsx"))
write.xlsx(gps , paste0("03_output/03_gps/gps_checks_2025-07-19.xlsx"))

# ──────────────────────────────────────────────────────────────────────────────
# 4. Apply the checks to the main data
# ──────────────────────────────────────────────────────────────────────────────



clog_metadata <- c("admin_1", "admin_2", "admin_3", "today","enum_id", "fo_in_charge", "index")
exclude_patterns <- c("geopoint", "gps", "_index", "_submit", "submission", "_sample_", "^_id$", "^rand", "^_index$","_n","enum_id",
                      "ind_potentially_hoh", "_person_id", "_ind_age", "index", "_photo_URL", "fo_in_charge", "uuid")
fcs_weight <- main_data %>% select(contains("fcs_weight")) %>% colnames()

## calculate other questions
others_to_check <- questions %>%
  filter(type == "text",
         str_detect(name, "other")) %>%
  pull(name)

df_list_logical_checks <- read_csv("02_input/01_logical_checks/main_data_checks.csv")

excluded_questions <- questions %>%
  filter(type != "integer" & type != "calculate") %>%
  filter(name != "muac_cm") %>% ## MUAC is a text field with regex for digits so need to manually remove
  pull(name) %>%
  unique()

outliers_exclude_main <- calc_outlier_exclusion(raw_kobo_data, excluded_q = excluded_questions, exclude_patt = exclude_patterns)

checked_main_data <-  main_data %>%
  run_standard_checks(survey = questions, choices = choices, value_check = TRUE,
                      outlier_check = TRUE,
                      columns_not_to_check_outliers = c(outliers_exclude_main, fcs_weight, "ind_aduld_above_18", "distance_to_site", "interview_duration"),
                      other_check = TRUE,
                      columns_to_check_others = names(main_data %>% select(any_of(others_to_check))),
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
  dplyr::select(admin_1, admin_2, admin_3, today,enum_id, resp_gender, enum_gender,
                hoh_gender,fo_in_charge,instance_name, index)


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

outliers_exclude_roster <- calc_outlier_exclusion(roster, excluded_q = c(excluded_questions, 'note_resp'), exclude_patt = exclude_patterns)

checked_hh_roster <- roster %>%
  run_standard_checks(
    survey = questions, choices = choices,
    value_check = TRUE,
    outlier_check = TRUE,
    columns_not_to_check_outliers = c(outliers_exclude_roster, "calc_final_age_years", "ind_pos", "ind_under5_age_years", "ind_under5_age_months"),
    other_check = FALSE,
    logical_check = FALSE)

hh_roster_cleaning_log <-  checked_hh_roster %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(information_to_add = clog_metadata)

# ──────────────────────────────────────────────────────────────────────────────
#  5.2 WGS data cleaning
# ──────────────────────────────────────────────────────────────────────────────

df_list_logical_wgs <- read_csv("02_input/01_logical_checks/wgs_logical_checks.csv")

checked_wgs <- wgs_repeat %>%
  filter(wgs_age > 4) %>%
  mutate(
    wgq_sum = rowSums(across(
      c(wgq_vision, wgq_hearing, wgq_mobility, wgq_cognition, wgq_self_care, wgq_communication),
      ~ .x %in% c("alot_difficulty", "cannot_all")), na.rm = TRUE)
  ) %>%
  run_standard_checks(
    survey = questions, choices = choices,
    value_check = FALSE,
    outlier_check = FALSE,
    other_check = FALSE,
    logical_check = TRUE,
    logical_list = df_list_logical_wgs)

checked_wgs_cleaning_log <- checked_wgs %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(information_to_add = clog_metadata)

# ──────────────────────────────────────────────────────────────────────────────
#  5.3 Child feeding data cleaning
# ──────────────────────────────────────────────────────────────────────────────

outliers_exclude_child_feeding <- calc_outlier_exclusion(child_feeding, excluded_q = c(excluded_questions, 'note_resp'), exclude_patt = exclude_patterns)

checked_child_feeding <- child_feeding %>%
  filter(!is.na(child_age_months)) %>%
  run_standard_checks(
    survey = questions, choices = choices,
    value_check = TRUE,
    outlier_check = TRUE,
    columns_not_to_check_outliers = c(outliers_exclude_child_feeding, "child_age", "child_age_months", "child_pos"),
    other_check = TRUE,
    columns_to_check_others = names(child_feeding %>% select(any_of(others_to_check))),
    logical_check = FALSE
)

child_feeding_cleaning_log <-  checked_child_feeding %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(information_to_add = clog_metadata)

# ──────────────────────────────────────────────────────────────────────────────
#  5.4 Child nutrition Roster data cleaning
# ──────────────────────────────────────────────────────────────────────────────

outliers_exclude_nut_ind <- calc_outlier_exclusion(nut_ind, excluded_q = excluded_questions, exclude_patt = exclude_patterns)

checked_nut_ind <- nut_ind %>%
  run_standard_checks(
    survey = questions, choices = choices,
    value_check = TRUE,
    outlier_check = TRUE,
    columns_not_to_check_outliers = c(outliers_exclude_nut_ind, "nut_person_id", "nut_ind_pos", "nut_ind_gender", "muac_calc"),
    other_check = TRUE,
    columns_to_check_others = names(nut_ind %>% select(any_of(others_to_check))),
    logical_check = FALSE
  )

nut_ind_cleaning_log <-  checked_nut_ind %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(information_to_add = clog_metadata)

# ──────────────────────────────────────────────────────────────────────────────
#  Mortality data cleaning
# ──────────────────────────────────────────────────────────────────────────────

outliers_exclude_mortality <- calc_outlier_exclusion(died_member, excluded_q = excluded_questions, exclude_patt = exclude_patterns)

died_member_checked <- died_member %>%
  run_standard_checks(
    survey = questions, choices = choices,
    value_check = TRUE,
    outlier_check = TRUE,
    columns_not_to_check_outliers = outliers_exclude_mortality,
    other_check = TRUE,
    columns_to_check_others = names(died_member %>% select(any_of(others_to_check))),
    logical_check = FALSE
    )

died_member_cleaning_log <-  died_member_checked %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(information_to_add = clog_metadata)

# ──────────────────────────────────────────────────────────────────────────────
#  6. Clog output
# ──────────────────────────────────────────────────────────────────────────────


final_clog <- bind_rows(
  main_cleaning_log$cleaning_log %>% mutate(clog_type = "main"),
  hh_roster_cleaning_log$cleaning_log %>% mutate(clog_type = "roster"),
  checked_wgs_cleaning_log$cleaning_log %>% mutate(clog_type = "wgs_repeat"),
  child_feeding_cleaning_log$cleaning_log %>% mutate(clog_type = "child_feeding"),
  nut_ind_cleaning_log$cleaning_log %>% mutate(clog_type = "nut_ind"),
  died_member_cleaning_log$cleaning_log %>% mutate(clog_type = "died_member")
  )


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
                                                                            "cleaning_log_",
                                                                            unique(.[]$checked_dataset$fo_in_charge),
                                                                            "_",
                                                                            date_time_now,
                                                                            ".xlsx")))
