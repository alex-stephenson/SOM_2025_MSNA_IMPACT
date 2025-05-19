
rm(list = ls())
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")

library(cleaningtools)
library(tidyverse)
library(readxl)
library(openxlsx)
library(ImpactFunctions)
library(robotoolbox)

raw_kobo <- ImpactFunctions::get_kobo_data(asset_id = "antAdT3siLrnjTdfcdYcFY", un = "alex_stephenson")


raw_kobo_data <- raw_kobo %>%
  pluck("main") %>%
  select(-uuid) %>%
  dplyr::rename(uuid =`_uuid`,
                index = `_index`) %>%
  distinct(uuid, .keep_all = T)

## extract all rosters
wgs <- raw_kobo %>%
  pluck("wgs_repeat") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`) %>%
  mutate(uuid = wgs_instance_name) %>%
  distinct(uuid, .keep_all = T) %>%
  select(any_of(c(contains("wgs"),contains("wgq"), contains("instance"), contains("index"),contains("uuid"))))

hh_roster <- raw_kobo %>%
  pluck("roster") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`) %>%
  mutate(uuid = person_id) %>%
  distinct(uuid, .keep_all = T)

edu_roster <- raw_kobo %>%
  pluck("edu_ind") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`)  %>%
  mutate(uuid = paste0("edu_", roster_index)) %>%
  select(any_of(c(contains("edu_"),contains("instance"), contains("index"),contains("uuid"))))


child_feeding <- raw_kobo %>%
  pluck("child_feeding") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`) %>%
  mutate(uuid = child_instance_name) %>%
  distinct(uuid, .keep_all = T)


health_roster <- raw_kobo %>%
  pluck("health_ind") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`)  %>%
  mutate(uuid = health_instance_name) %>%
  distinct(uuid, .keep_all = T)


child_vacination_roster <- raw_kobo %>%
  pluck("child_vacination") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`) %>%
  mutate(uuid = vacination_instance_name) %>%
  distinct(uuid, .keep_all = T) %>%
  select(c(-contains('cm_income'), -contains('prot_')))

nut_ind_roster <- raw_kobo %>%
  pluck("nut_ind") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`) %>%
  mutate(uuid = nut_person_id) %>%
  distinct(uuid, .keep_all = T)


died_member_roster <- raw_kobo %>%
  pluck("died_member") %>%
  rename(index = `_parent_index`,
         roster_index = `_index`)  %>%
  mutate(uuid = paste0("mortality_", roster_index))



list_all_data <- list("main" = raw_kobo_data, "WGS" = wgs, "hh_roster" = hh_roster,
                      "education" = edu_roster, "child_feeding" = child_feeding,
                      "health" = health_roster, "child_vaccination" = child_vacination_roster,
                      "nutrition" = nut_ind_roster, "mortality" = died_member_roster)


list_all_data %>%
  write_rds(., "03_output/01_raw_data/all_raw_data.rds")

version_count <- n_distinct(raw_kobo_data$`__version__`)
if (version_count > 1) {
  print("~~~~~~There are multiple versions of the tool in use~~~~~~")
}


# read in the survey questions / choices

kobo_tool_name <- "04_tool/REACH_2024_MSNA_kobo tool.xlsx"
questions <- read_excel(kobo_tool_name, sheet = "survey")
choices <- read_excel(kobo_tool_name, sheet = "choices")

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  select(admin_2, fo_in_charge = FO_In_Charge)

# # join the fo to the dataset
data_with_fo <- raw_kobo_data %>%
  left_join(fo_district_mapping, by = "admin_2")

###################define initiatlized variables

mindur <- 40
maxdur <- 90


## data prep

data_with_fo <- data_with_fo %>%
  mutate(settlement_idp=
         case_when(
           admin_3 == "settlement" ~ settlement,
           admin_3 == "idp_sites" ~ idp_site,
           TRUE ~ NA
         ),
       final_hoh_gender=
         case_when(
           resp_hoh_yn == "yes" ~ resp_gender,
           resp_hoh_yn == "no" ~ hoh_gender,
           TRUE ~ NA
         )) %>%
  relocate(settlement_idp, .after = "admin_3") %>%
  relocate(final_hoh_gender, .after = "resp_hoh_yn")

### Metadata query

set.seed(123)
data_with_time <- data_with_fo %>%
  mutate(interview_duration = runif(nrow(data_with_fo), 39, 91))

kobo_data_metadata <- get_kobo_metadata(dataset = data_with_fo, un = "alex_stephenson", asset_id = "antAdT3siLrnjTdfcdYcFY", remove_geo = T)

data_with_time <- kobo_data_metadata$df_and_duration

raw_metadata_length <- kobo_data_metadata$audit_files_length

write_rds(raw_metadata_length, "03_output/01_raw_data/raw_metadata.rds")


data_in_processing <- data_with_time %>%
  mutate(length_valid = case_when(
    interview_duration < mindur ~ "Too short",
    interview_duration > maxdur ~ "Too long",
    TRUE ~ "Okay"
  ))


deletion_log <- data_in_processing %>%
  filter(length_valid != "Okay") %>%
  select(uuid, length_valid, settlement_idp, enum_id, interview_duration) %>%
  left_join(raw_kobo_data %>% select(uuid, index), by = "uuid")


deletion_log %>%
  mutate(comment = paste0("Interview length is ", length_valid)) %>%
  select(-length_valid) %>%
  writexl::write_xlsx(., paste0("03_output/02_deletion_log/deletion_log.xlsx"))

## filter only valid surveys and for the specific date
data_valid_date <- data_in_processing %>%
  filter(length_valid == "Okay") %>%
  filter(today == "2024-06-10")



###### read all settlements #################

master_settlement <- read_excel("02_input/05_site_data/Site_Master_List.xlsx")
main_data <- data_valid_date %>% left_join(master_settlement, by ="settlement_idp")###join sample name with main data

## can we find a way to add UoA, hexid and cluster ID In? Based on whether it's idp or not and region / district selected.


############### GIS ########################

gis_data <- main_data %>%
  select(uuid,admin_1,admin_2,admin_3,settlement_idp,Name,contains("_household_geopoint"),enum_name,Longitude_samp,Latitude_samp,
         dist_diff,fo_in_charge,today,Pcode) %>%
  dplyr::rename(adm1_rep= admin_1, adm2_rep= admin_2, adm3_rep= admin_3, pcode_rep = settlement_idp,
                                                               name_rep= Name, hh_lat = `_household_geopoint_latitude`, hh_long = `_household_geopoint_longitude`,
                                                               hh_alt = `_household_geopoint_altitude`, hh_prec = `_household_geopoint_precision`,
                                                               samp_long = Longitude_samp, samp_lat = Latitude_samp, FO_resp = fo_in_charge,date= today)


gis_data %>%
  writexl::write_xlsx(., paste0("03_output/03_gps/gps_checks_",today(),".xlsx"))

#------------------------------------------------------------------------#
##################impor sample target to target calculation###############
#------------------------------------------------------------------------#

sample <- read.csv("01_inputs/04_Sample/Samples.csv")

################################################################################
dashboard_data <- main_data


## dashboard data prep

dashboard_data <- dashboard_data %>%
  filter(consent=="yes") %>%
  mutate(district_pop=ifelse(place_of_origin=="yes", paste0(district,"_hc"),
                             ifelse(place_of_origin == "no" & time_of_movement== "less_one_year",paste0(district,"_new_idp"),
                                    ifelse(place_of_origin == "no" & time_of_movement== "more_one_year",paste0(district,"_protracted"),NA))),
         pop_status=ifelse(place_of_origin=="yes", paste0("Host Community"),
                           ifelse(place_of_origin == "no" & time_of_movement== "less_one_year",paste0("New IDP"),
                                  ifelse(place_of_origin == "no" & time_of_movement== "more_one_year",paste0("Protracted"),NA))),
         Deleted= ifelse(interview_duration<40,paste0(1),0),
         Retained= ifelse(interview_duration>=40,paste0(1),0)) %>%
  select(uuid,region,district,admin_3,interview_duration,
         settlement_idp,resp_gender,contains("_household_geopoint"),
         Deleted,Retained,district_pop,pop_status,enum_id,fo_in_charge) %>%
  dplyr::rename(`Respondent Gender` = resp_gender, idp_settlement = admin_3,`_Gps Latitude` = `_household_geopoint_latitude`, `_Gps Longitude` = `_household_geopoint_longitude`, `_gps2_altitude` =	`_household_geopoint_altitude`,
                `_GpsPprecision` = `_household_geopoint_precision`
  )


dashboard_data <- dashboard_data %>% left_join(sample, by = "uuid")



#------------------------------------------------------------------------#
############################ data quality checks #########################
#------------------------------------------------------------------------#

source("00_src/utils.R")

## calculate oultiers questions
excluded_questions <- questions %>%
  filter(type != "integer") %>%
  pull(name) %>%
  unique()

excluded_questions_in_data <- intersect(colnames(main_data), excluded_questions)

gps_questions <- main_data %>% select(contains("geopoint")) %>% colnames(.)

checked_main_data <-  main_data %>%
  check_duplicate(
    uuid_column = "uuid",
    columns_to_check = NULL) %>%
  check_pii(
       element_name = "checked_dataset",
       uuid_column = "uuid") %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(
      main_data |>
        dplyr::select(ends_with("_other")) |>
        dplyr::select(-contains("."))
    )) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)) %>%
  check_outliers(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    kobo_survey = questions,
    kobo_choices = choices,
    cols_to_add_cleaning_log = NULL,
    strongness_factor = 3,
    minimum_unique_value_of_variable = NULL,
    remove_choice_multiple = TRUE,
    sm_separator = "/",
    columns_not_to_check = c(excluded_questions_in_data, "index", "enum_id","enum_age", "interview_duration", "length_valid",
                             gps_questions, "Longitude_samp","Latitude_samp","_id", "fsl_fcs_cereals",
                             "fsl_fcs_dairy","fsl_fcs_vitA_veg", "fsl_fcs_green_veg","fsl_fcs_veg","fsh_fcs_vitA_fruits", "fsl_fcs_fruit",
                             "fsl_fcs_organ_meat","fsl_fcs_meat","fsl_fcs_eggs","fsl_fcs_fish","fsl_fcs_legumes","fsl_fcs_roots","fsl_fcs_oil",
                             "fsl_fcs_sugar","fsl_fcs_condiments","fsl_rcsi_lessexpensive","fsl_rcsi_borrow", "fsl_rcsi_mealnb",
                             "fsl_rcsi_mealsize", "fsl_rcsi_mealadult","fsl_rcsi_lessquality","num_left","num_join"
    )
  ) %>%
  check_logical_with_list( uuid_column = "uuid",
                           list_of_check = main_check_list,
                           check_id_column = "name",
                           check_to_perform_column = "check",
                           columns_to_clean_column = "columns_to_clean",
                           description_column = "description"
  )

main_cleaning_log <-  checked_main_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )



#--------------------------------------------------------------------------------------------------------#
###################################~~~ Roster data cleaning ~~~ ####################################
#------------------------------------------------------------------------------------------------------#

main_to_join <- main_data %>%
  dplyr::select(admin_1,admin_2,today,enum_id,resp_gender, enum_gender,
                hoh_gender,settlement_idp,fo_in_charge,Name,deviceid,instance_name, index)


trans_roster <- function(data, id_name) {
  data %>%
    dplyr::filter(!.data$index %in% deletion_log$index) %>%
    dplyr::filter(.data$index %in% main_data$index) %>%
    dplyr::left_join(main_to_join, by = join_by(`index` == index)) %>%
    dplyr::filter(!is.na(.data$fo_in_charge)) #%>% ### THIS WOULD NEED REMOVING
 #   dplyr::rename(uuid = {{ id_name }})
}

#--------------------------------------------------------------------------------------------------------#
###################################### HH Roaster data cleaning  ########################################
#------------------------------------------------------------------------------------------------------#



hh_roster_bad_removed <- hh_roster %>%
  trans_roster()

checked_hh_roster_data <- hh_roster_bad_removed %>%
  check_duplicate(
    uuid_column = "uuid"
  ) %>%
  check_logical_with_list( uuid_column = "uuid",
                                   list_of_check = hh_check_list,
                                   check_id_column = "name",
                                   check_to_perform_column = "check",
                                   columns_to_clean_column = "columns_to_clean",
                                   description_column = "description"
  )


roster_cleaning_log <-  checked_hh_roster_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )



#------------------------------------------------------------------------#
############################  wgs cleaning is here #######################
#------------------------------------------------------------------------#


wgs_bad_removed <- wgs %>%
  trans_roster()

wgs_bad_removed <- wgs_bad_removed %>%
  mutate(
    calc_wgq_vision = case_when(
      wgq_vision == "alot_difficulty" | wgq_vision == "cannot_all"  ~ 1,
    ),
    calc_wgq_hearing = case_when(
      wgq_hearing == "alot_difficulty" | wgq_hearing == "cannot_all"  ~ 1,
    ),

    calc_wgq_mobility = case_when(
      wgq_mobility == "alot_difficulty" | wgq_mobility == "cannot_all"  ~ 1,
    ),
    calc_wgq_cognition = case_when(
      wgq_cognition == "alot_difficulty" | wgq_cognition == "cannot_all"  ~ 1,
    ),

    calc_wgq_self_care = case_when(
      wgq_self_care == "alot_difficulty" | wgq_self_care == "cannot_all"  ~ 1,
    ),
    calc_wgq_communication = case_when(
      wgq_communication == "alot_difficulty" | wgq_communication == "cannot_all"  ~ 1,
    )

  )

wgs_bad_removed <- wgs_bad_removed %>%
  mutate( number_of_disabilities = rowSums(select(., calc_wgq_vision, calc_wgq_hearing, calc_wgq_mobility, calc_wgq_cognition,
                                                  calc_wgq_self_care, calc_wgq_communication), na.rm=T)
  )

checked_wgs_data <- wgs_bad_removed %>%
  check_logical_with_list( uuid_column = "uuid",
                           list_of_check = wgs_check_list,
                           check_id_column = "name",
                           check_to_perform_column = "check",
                           columns_to_clean_column = "columns_to_clean",
                           description_column = "description"
  ) ### why are no other checks done here


wgs_cleaning_log <-  checked_wgs_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )



#------------------------------------------------------------------------#
################### HH Education data cleaning  ##########################
#------------------------------------------------------------------------#

edu_roster_bad_removed <- edu_roster %>%
  trans_roster()

checked_education_data <-  edu_roster_bad_removed %>%
  check_duplicate(
    uuid_column = "uuid"
  )  %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(edu_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  ) %>%
  check_logical_with_list( uuid_column = "uuid",
                           list_of_check = edu_check_list,
                           check_id_column = "name",
                           check_to_perform_column = "check",
                           columns_to_clean_column = "columns_to_clean",
                           description_column = "description"
  )

edu_cleaning_log <-  checked_education_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )

#------------------------------------------------------------------------#
############### child_feeding data cleaning ##############################
#------------------------------------------------------------------------#

child_feeding_bad_removed <- child_feeding %>%
  trans_roster(child_id) %>%
  filter(caregiver_consent=="yes" | caregiver_consent=="no")

child_feeding_formatted <- format_nut_health_indicators(df = child_feeding_bad_removed, ## why is some of this commented out?
                                                        use_flags_yn = "yes",
                                                        hhid = "uuid",
                                                        date_of_dc = "today",
                                                        enum = "enum_id",
                                                        # cluster = "cluster_id",
                                                        # date_of_birth = "date_of_birth_of_name",
                                                        age_months_var = "child_age_months",
                                                        sex_var = "child_gender",
                                                        iycf_1 = "breastfed_ever", # ever breastfed (y/n)
                                                        #iycf_2 = "first_put_to_breast", # how long after birth breastfed
                                                        #iycf_3 = "breastmilk_first_two_days", # given anything to eat/drink in first 2 days after delivery
                                                        iycf_4 = "breastfed_yesterday", # breastfed yesterday during the day or night (y/n)
                                                        #iycf_5 = "bottle_with_nipple", # drink anything from bottle with a nipple (y/n)
                                                        iycf_6a = "water", # plain water
                                                        iycf_6b = "infant_formula_yesterday_yn", # infant formula (y/n)
                                                        iycf_6b_num = "infant_formula", # infant formula (number)
                                                        iycf_6c = "milk_yn", # milk from animals, fresh tinned powder (y/n)
                                                        iycf_6c_num = "milk", # milk form animals, fresh, tinned, pwder (number)
                                                        #iycf_6c_swt = "sweet_or_flavoured_type_of_milk", # milk was sweetened (y/n)
                                                        iycf_6d = "sour_milk_yn", # yoghurt drinks (y/n)
                                                        iycf_6d_num = "sour_milk", # yoghurt drinks (number)
                                                        #iycf_6d_swt = "sweet_or_flavoured_type_of_yogurt_drink", # yoghurt drink was sweetened (y/n)
                                                        iycf_6e = "choco_drink", # chocolate flavoured drinks, including from syrup / powders (y/n)
                                                        iycf_6f = "juice", # Fruit juice or fruit-flavoured drinks including those made from syrups or powders? (y/n)
                                                        iycf_6g = "soda", # sodas, malt drinks, sports and energy drinks (y/n)
                                                        iycf_6h = "tea", # tea, coffee, herbal drinks (y/n)
                                                        #iycf_6h_swt = "any_of_these_otherliquids_sweetened", # tea coffee herbal drinks were sweetened (y/n)
                                                        iycf_6i = "clear_broth", # clear broth / soup (y/n)
                                                        iycf_6j = "water_based", # other liquids (y/n)
                                                        #iycf_6j_swt = "any_of_these_otherliquids_sweetened", # other drinks were sweetened (y/n)
                                                        # iycf_7a = "nutrition_Yoghurt_yes_no", # yoghurt (NOT yoghurt drinks) (y/n)
                                                        iycf_7a_num = "yoghurt", # yoghurt (NOT yoghurt drinks) (number)
                                                        iycf_7b = "cereals", # porridge, bread, rice, nooodles (y/n)
                                                        iycf_7c = "vegetables", # vitamin a rich vegetables (pumpkin, carrots, sweet red peppers, squash or yellow/orange sweet potatoes) (y/n)
                                                        iycf_7d = "root_vegetables", # white starches (plaintains, white potatoes, white yams, manioc, cassava) (y/n)
                                                        iycf_7e = "leafy_vegetables", # dark green leafy vegetables (y/n)
                                                        iycf_7f = "vegetables_other", # other vegetables (y/n)
                                                        iycf_7g = "tropical_fruits", # vitamin a rich fruits (ripe mangoes, ripe papayas) (y/n)
                                                        iycf_7h = "fruits_other", # other fruits (y/n)
                                                        iycf_7i = "organ_meats", # organ meats (liver ,kidney, heart) (y/n)
                                                        iycf_7j = "canned_meat", # processed meats (sausages, hot dogs, ham, bacon, salami, canned meat) (y/n)
                                                        iycf_7k = "meat", # any other meats (beef, chicken, pork, goat, chicken, duck) (y/n)
                                                        iycf_7l = "egg", # eggs (y/n)
                                                        iycf_7m = "seafood", # fish (fresh or dried fish or shellfish) (y/n)
                                                        iycf_7n = "legumes", # legumes (beans, peas, lentils, seeds, chickpeas) (y/n)
                                                        iycf_7o = "cheese", # cheeses (hard or soft cheeses) (y/n)
                                                        iycf_7p = "sweets", # sweets (chocolates, candies, pastries, cakes) (y.n)
                                                        iycf_7q = "chips", # fried or empty carbs (chips, crisps, french fries, fried dough, instant noodles) (y/n)
                                                        iycf_7r = "foods_other", # Any other solid, semi-solid, or soft foods
                                                        #iycf_7s = "any_other_solid_semi_solid_or_soft_food", # did child eat solid/semi-solid foods (y/n) for list based questionnaires
                                                        iycf_8 = "child_solid_yesterday", # times child ate solid/semi-solid foods (number)
)


### i think here i should just make own clogs, but i dont know where this function comes from

# 3. Generate flags
child_feeding_flags <- create_cleaning_log_flags(
  df = child_feeding_formatted,
  uuid_col = "hh_id"
)

# 4. Create lookup table for question renaming
question_rename_lookup <- c(
  iycf_1 = "breastfed_ever", iycf_4 = "breastfed_yesterday",
  iycf_6a = "water", iycf_6b = "infant_formula_yesterday_yn", iycf_6b_num = "infant_formula",
  iycf_6c = "milk_yn", iycf_6c_num = "milk", iycf_6d = "sour_milk_yn", iycf_6d_num = "sour_milk",
  iycf_6e = "choco_drink", iycf_6f = "juice", iycf_6g = "soda", iycf_6h = "tea",
  iycf_6i = "clear_broth", iycf_6j = "water_based", iycf_7a = "yoghurt_yn", iycf_7a_num = "yoghurt",
  iycf_7b = "cereals", iycf_7c = "vegetables", iycf_7d = "root_vegetables", iycf_7e = "leafy_vegetables",
  iycf_7f = "vegetables_other", iycf_7g = "tropical_fruits", iycf_7h = "fruits_other", iycf_7i = "organ_meats",
  iycf_7j = "canned_meat", iycf_7k = "meat", iycf_7l = "egg", iycf_7m = "seafood",
  iycf_7n = "legumes", iycf_7o = "cheese", iycf_7p = "sweets", iycf_7q = "chips", iycf_7r = "foods_other",
  iycf_8 = "child_solid_yesterday"
)

# 5. Create function for converting numeric responses to labels
convert_numeric_to_label <- function(question, value) {
  if (value %in% c(1, 2, 9)) {
    label_map <- c("1" = "yes", "2" = "no", "9" = "pnta")
    return(label_map[as.character(value)])
  }
  return(value)
}

# 6. Apply renaming and recoding
child_feeding_flags_renamed <- child_feeding_flags %>%
  mutate(
    question.name = coalesce(question_rename_lookup[question.name], question.name),
    old.value = if_else(
      question.name %in% c("caregiver_consent", "breastfed_ever", "breastfed_yesterday", "water",
                           "infant_formula_yesterday_yn", "milk_yn", "yoghurt_yn", "sour_milk_yn",
                           "choco_drink", "juice", "soda", "tea"),
      convert_numeric_to_label(question.name, old.value),
      old.value
    )
  )

child_feeding_flags_renamed <-child_feeding_flags_renamed %>%
  select(-c("issue","feedback")) %>%
  dplyr::rename(question = question.name,
                old_value = old.value,
                new_value = new.value,
                issue = description,
                change_type= changed) %>%
  mutate(check_binding= paste0(question,uuid))


#------------------------------------------------------------------------#
################## HH Health data cleaning ###############################
#------------------------------------------------------------------------#

health_roster_bad_removed <- health_roster %>%
  trans_roster()

checked_health_data <-health_roster_bad_removed %>%
  check_duplicate(
    uuid_column = "uuid"
  ) %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(health_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))
  ) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  )


health_cleaning_log <-  checked_health_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )


#------------------------------------------------------------------------#
##################### HH child_vacination data cleaning ##################
#------------------------------------------------------------------------#

child_vacination_roster_bad_removed <- child_vacination_roster %>% #remove surveys that will be deleted
  trans_roster()

checked_child_vacination_data <-child_vacination_roster_bad_removed %>%
  check_duplicate(
    uuid_column = "uuid"
  ) %>%
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(child_vacination_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))
  ) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  )

child_vacination_cleaning_log <-  checked_child_vacination_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )

#------------------------------------------------------------------------#
#################### HH died_member data cleaning ########################
#------------------------------------------------------------------------#

died_member_roster_bad_removed <- died_member_roster %>%
  trans_roster()

checked_died_member_data <-died_member_roster_bad_removed %>%
  check_duplicate(
    uuid_column = "uuid"
  ) %>%
#  check_others(
#    uuid_column = "uuid",
#    columns_to_check = names(died_member_roster_bad_removed|>
#                               dplyr::select(ends_with("_other")) |>
#                               dplyr::select(-contains(".")))
#  ) %>%
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  ) %>%
  check_logical_with_list( uuid_column = "uuid",
                           list_of_check = mortality_check_list,
                           check_id_column = "name",
                           check_to_perform_column = "check",
                           columns_to_clean_column = "columns_to_clean",
                           description_column = "description"
  )

died_cleaning_log <-  checked_died_member_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("enum_id", "enum_gender", "fo_in_charge", "admin_1", "admin_2", "settlement_idp", "index")
  )

#------------------------------------------------------------------------#
########################## fcs data ######################################
#------------------------------------------------------------------------#

### this ultimately gets added in but i dont know why / whether we can just replace with our own checks

follow_up <- read_excel("C:/Users/aaron.langat/Documents/R/03_MSNA/01_Data_Cleaning/fsl_cleaning/output/FSL_followup_requests.xlsm", sheet = "Follow-up")
cleaning_logbook <- read_excel("C:/Users/aaron.langat/Documents/R/03_MSNA/01_Data_Cleaning/fsl_cleaning/output/cleaning_logbook.xlsx")

fcs_data <- rbind.fill(follow_up,cleaning_logbook)

fcs_data<- fcs_data %>%
  dplyr::rename(question= variable, old_value=old.value, old_value=old.value, new_value=new.value,check_id = check.id) %>%
  select(-c("invalid","explanation")) %>%
  mutate(check_binding= paste0(question,uuid))

## metadata added here so clogs are standardised
fcs_data$admin_1 <- main_data$admin_1[match(fcs_data$uuid, main_data$uuid)]#region
fcs_data$admin_2 <- main_data$admin_2[match(fcs_data$uuid, main_data$uuid)]#district
fcs_data$today <- main_data$today[match(fcs_data$uuid, main_data$uuid)]#today
# fcs_data$enum_id <- main_data$enum_id[match(fcs_data$parent_instance_name, main_data$instance_name)]#enum_name
fcs_data$settlement_idp <- main_data$settlement_idp[match(fcs_data$uuid, main_data$uuid)]#hoh gender
fcs_data$fo_in_charge <- main_data$fo_in_charge[match(fcs_data$uuid, main_data$uuid)]#Fo name and base
fcs_data$Name <- main_data$Name[match(fcs_data$uuid, main_data$uuid)]#Name of the settlement
fcs_data$deviceid <- main_data$deviceid[match(fcs_data$uuid, main_data$uuid)]#Name of the deviceid



#------------------------------------------------------------------------#
########################## output clogs ##################################
#------------------------------------------------------------------------#

final_clog <- bind_rows(
  main_cleaning_log$cleaning_log %>% mutate(clog_type = "main"),
  roster_cleaning_log$cleaning_log %>% mutate(clog_type = "hh_roster"),
  child_vacination_cleaning_log$cleaning_log %>% mutate(clog_type = "child_vaccination"),
  edu_cleaning_log$cleaning_log %>% mutate(clog_type = "education"),
  health_cleaning_log$cleaning_log %>% mutate(clog_type = "health"),
  #child_feeding_flags_renamed,
  died_cleaning_log$cleaning_log %>% mutate(clog_type = "mortality"),
  wgs_cleaning_log$cleaning_log %>% mutate(clog_type = "WGS"))#,
  #fcs_data)

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
cleaning_log %>% purrr::map(~ cleaningtools::create_xlsx_cleaning_log(.[],
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




# Rows and columns to blank out

words_to_replace <- c("dnk","pnta")

# Replace the words with blank
main_data <- main_data %>%
  mutate(across(
    c(fsl_hhs_nofoodhh, fsl_hhs_sleephungry, fsl_hhs_alldaynight),
    ~ str_replace_all(., regex(paste(words_to_replace, collapse = "|")), "no")
  ))

# # convert integers columns to numeric
cols.integer <- filter(tool.survey, type=="integer")$name
main_data <- mutate_at(main_data, cols.integer, as.numeric)


# #convert select_one to character
cols.characters <- filter(tool.survey,str_detect(type, "select_one"))$name
main_data <- mutate_at(main_data, cols.characters, as.character)


main_data <- main_data %>% add_fcs(
  cutoffs = "normal",
  fsl_fcs_cereal = "fsl_fcs_cereals",
  fsl_fcs_legumes = "fsl_fcs_legumes",
  fsl_fcs_veg = "fsl_fcs_veg",
  fsl_fcs_fruit = "fsl_fcs_fruit",
  fsl_fcs_meat = "fsl_fcs_meat",
  fsl_fcs_dairy = "fsl_fcs_dairy",
  fsl_fcs_sugar = "fsl_fcs_sugar",
  fsl_fcs_oil = "fsl_fcs_oil" ) %>%
  # ) %>%add_lcsi(
  #   fsl_lcsi_stress1 = "fsl_lcsi_stress1",
  #   fsl_lcsi_stress2 = "fsl_lcsi_stress2",
  #   fsl_lcsi_stress3 = "fsl_lcsi_stress3",
  #   fsl_lcsi_stress4 = "fsl_lcsi_stress4",
  #   fsl_lcsi_crisis1 = "fsl_lcsi_crisis1",
  #   fsl_lcsi_crisis2 = "fsl_lcsi_crisis2",
  #   fsl_lcsi_crisis3 = "fsl_lcsi_crisis3",
  #   fsl_lcsi_emergency1 = "fsl_lcsi_emergency1",
  #   fsl_lcsi_emergency2 = "fsl_lcsi_emergency2",
  #   fsl_lcsi_emergency3 = "fsl_lcsi_emergency3",
  #   yes_val = "yes",
  #   no_val = "no_had_no_need",
  #   exhausted_val = "no_exhausted",
  #   not_applicable_val = "not_applicable"
  # ) %>%
  add_rcsi(
    fsl_rcsi_lessquality = "fsl_rcsi_lessquality",
    fsl_rcsi_borrow = "fsl_rcsi_borrow",
    fsl_rcsi_mealsize = "fsl_rcsi_mealsize",
    fsl_rcsi_mealadult = "fsl_rcsi_mealadult",
    fsl_rcsi_mealnb = "fsl_rcsi_mealnb"
  ) %>% add_hdds(
    fsl_hdds_cereals = "fsl_hdds_cereals",
    fsl_hdds_tubers = "fsl_hdds_roots",
    fsl_hdds_veg = "fsl_hdds_veg",
    fsl_hdds_fruit = "fsl_hdds_fruit",
    fsl_hdds_meat = "fsl_hdds_meat",
    fsl_hdds_eggs = "fsl_hdds_eggs",
    fsl_hdds_fish = "fsl_hdds_fish",
    fsl_hdds_legumes = "fsl_hdds_legumes",
    fsl_hdds_dairy = "fsl_hdds_dairy",
    fsl_hdds_oil = "fsl_hdds_oil",
    fsl_hdds_sugar = "fsl_hdds_sugar",
    fsl_hdds_condiments = "fsl_hdds_condiments"
  )%>% add_hhs(
    fsl_hhs_nofoodhh = "fsl_hhs_nofoodhh",
    fsl_hhs_nofoodhh_freq = "fsl_hhs_nofoodhh_freq",
    fsl_hhs_sleephungry = "fsl_hhs_sleephungry",
    fsl_hhs_sleephungry_freq = "fsl_hhs_sleephungry_freq",
    fsl_hhs_alldaynight = "fsl_hhs_alldaynight",
    fsl_hhs_alldaynight_freq = "fsl_hhs_alldaynight_freq",
    yes_answer = "yes",
    no_answer = "no",
    rarely_answer = "rarely",
    sometimes_answer = "sometimes",
    often_answer = "often"
  )


#############Clean mortality data####household_cl
clog_input <- read_excel("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/002_Cleaning logbook/SOM_Mortality_cleaning_logbook.xlsx",sheet = "household_cl")
clog_input <- clog_input %>%
  filter(change_type != "waiting_followup")
main_data <- create_clean_data(
  main_data,
  raw_data_uuid_column = "uuid",
  clog_input,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column="question.name",
  cleaning_log_new_value_column="new.value",
  cleaning_log_change_type_column="change_type",
  change_response_value = "change_response",
  NA_response_value = "blank_response",
  no_change_value = "no_action",
  remove_survey_value = "remove_survey"
)


#############Clean mortality data ## this can all be removed once i confirm what was being done by HQ

clog_ded <- read_excel("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/002_Cleaning logbook/SOM_Mortality_cleaning_logbook.xlsx",sheet = "died_cl")

clog_ded <- clog_ded %>%
  filter(change_type != "waiting_followup")

died_member_roster_bad_removed <- create_clean_data(
  died_member_roster_bad_removed,
  raw_data_uuid_column = "_index",
  clog_ded,
  cleaning_log_uuid_column = "_index",
  cleaning_log_question_column="question.name",
  cleaning_log_new_value_column="new.value",
  cleaning_log_change_type_column="change_type",
  change_response_value = "change_response",
  NA_response_value = "blank_response",
  no_change_value = "no_action",
  remove_survey_value = "remove_survey"
)

#### household cl
clog_roster <- read_excel("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/002_Cleaning logbook/SOM_Mortality_cleaning_logbook.xlsx",sheet = "roster_cl")

clog_roster <- clog_roster %>%
  filter(change_type != "waiting_followup")

hh_roster_bad_removed <- create_clean_data(
  hh_roster_bad_removed,
  raw_data_uuid_column = "_index",
  clog_ded,
  cleaning_log_uuid_column = "_index",
  cleaning_log_question_column="question.name",
  cleaning_log_new_value_column="new.value",
  cleaning_log_change_type_column="change_type",
  change_response_value = "change_response",
  NA_response_value = "blank_response",
  no_change_value = "no_action",
  remove_survey_value = "remove_survey"
)


#####################Export downloaded data
combined_data <-list("SOM2404_MSNA_Tool" = main_data,"roster" = hh_roster_bad_removed,"wgs_repeat" = wgs_bad_removed,"edu_ind" = edu_roster_bad_removed,
                     "child_feeding" = child_feeding_bad_removed, "health_ind" = health_roster_bad_removed, "child_vacination" = child_vacination_roster_bad_removed,
                     "died_member" = died_member_roster_bad_removed
)

raw_final_data <- paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/00_Raw Data/SOM2404_MSNA_Tool_",today,".xlsx")
write.xlsx(combined_data, raw_final_data, rowNames = FALSE)
