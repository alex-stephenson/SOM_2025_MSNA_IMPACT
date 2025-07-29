# ─────────────────────────────────────────────────────────────────────────────────
# This code uses the Somalia standardised approach for producing clogs, applying
# them and making raw, clean, clogs and dlogs (deletion logs) on a daily basis.
#
# Within the MSNA framework it is more complicated because we have to allow for
# multiple repeat sections within the data. This code extensively uses iterative
# code to achieve this, primarily from the {purrr} library.
#
# The code basically follows these steps
# ➡️ Read in  raw data, clogs and deletion logs
# ➡️ splits the raw data and clogs into a list of dataframes, where we have
# one element within the list for each repeat section in the tool, and also a 'main'
# section.
# ➡️ Review each nested cleaning log and data combination, apply the cleaning logs
# and then apply the recreate parent column if there is any select multiple
# questions.
# ➡️ The created clogs are also reviewed on a daily basis. Again this happens
# itertively.
# ➡️ If there are no clogs identified, most of these steps are skipped.
# ➡️ Finally, all of the outputs outputted, and then can be fed into the field
# monitoring dashboard.
# ─────────────────────────────────────────────────────────────────────────────────

rm(list = ls())

library(tidyverse)
library(cleaningtools)
library(readxl)
library(ImpactFunctions)
library(addindicators)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Read in all data (raw, clogs, FO, tool)
# ──────────────────────────────────────────────────────────────────────────────

geo_ref_data <- readxl::read_excel("02_input/07_geo_reference_data/sample_frame_SOM_MSNA_2025.xlsx", sheet = "sample_frame_SOM_MSNA_2025") %>%
  select(unit_of_analysis, district, admin_2, hex_ID, survey_buffer)

point_data <- read_csv("04_tool/sample_points.csv") %>%
  distinct() %>%
  select(Hex_ID, point_id = Point_ID)

# read in the FO/district mapping
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  rename(fo_in_charge = FO_In_Charge)

## raw data
all_raw_data <- read_rds("03_output/01_raw_data/all_raw_data.rds")

## tool
kobo_tool_name <- "../../01_MSNA 2025_ Research Design/REACH_SOM2503_MSNA_2025_IPC.xlsx"
kobo_survey <- read_excel(kobo_tool_name, sheet = "survey")
kobo_choice <- read_excel(kobo_tool_name, sheet = "choices")

# Read in all the clogs
file_list <- list.files(path = "01_cleaning_logs", recursive = TRUE, full.names = TRUE)

file_list <- file_list %>%
  keep(~ str_detect(.x, "complete")) %>%
  keep(~ str_detect(.x, "validated"))

# Function to read and convert all columns to character
read_and_clean <- function(file, sheet) {
  read_excel(file, sheet = sheet, col_types = "text") %>%
    mutate(
      across(everything(), as.character),
      old_value = str_replace(str_trim(old_value), "\\.0+$", ""),
      new_value = str_replace(str_trim(new_value), "\\.0+$", ""),
      file_path = file
    )
}


# ──────────────────────────────────────────────────────────────────────────────
# 2. Split the code - the following process runs if we have identified any clogs.
#    If there are no clogs, then most of this gets skipped and instead we just
#    produce a raw and clean data output using the deletion log.
# ──────────────────────────────────────────────────────────────────────────────


if(! is_empty(file_list)) {

  message("Cleaning logs identified, completing full script")

  Sys.sleep(2)

  # Read and combine all files into a single dataframe
  cleaning_logs <- map_dfr(file_list, sheet = 'cleaning_log', read_and_clean)

  cleaning_logs <- cleaning_logs %>%
    filter(! is.na(change_type)) %>%
    mutate(new_value = ifelse(change_type == "no_action" & is.na(new_value), old_value, new_value)) %>%
    filter(!str_detect(question, "fcs_weight_")) %>% ## these questions included on first day but shouldnt be
    filter(question != "fsl_fcs_score") %>% ## I dont want to include changes to FCS, we'll just recalulcate it
    filter(question != "interview_duration")

  ## now we split the clogs by the clog type, we manually coded at the end of the last script.

  clogs_split <- cleaning_logs %>%
    group_by(clog_type) %>%
    group_split() %>%
    set_names(map_chr(., ~ unique(.x$clog_type)))


  common_rosters <- intersect(
    names(all_raw_data),
    cleaning_logs$clog_type %>% unique() %>% na.omit()
  )

  list_of_df_and_clog <- map(common_rosters, function(g) {
    raw <- all_raw_data[[g]]
    clog <- clogs_split[[g]] %>% mutate(across(everything(), as.character))

    list(
      raw_data = raw,
      cleaning_log = clog
    )
  })



  names(list_of_df_and_clog) <- common_rosters

  message("✅ Clogs successfully split")
  Sys.sleep(2)

  # Read in all the dlogs
  all_dlogs <- readxl::read_excel(r"(03_output/02_deletion_log/deletion_log.xlsx)", col_types = "text")
  manual_dlog <- readxl::read_excel("03_output/02_deletion_log/MSNA_2025_Manual_Deletion_Log.xlsx", col_types = "text")

  cleaning_log_deletions <- cleaning_logs %>%
    filter(change_type == "remove_survey") %>%
    mutate(interview_duration = "") %>%
    select(uuid, admin_1, admin_2, admin_3, enum_id,interview_duration, index, comment = issue) %>%
    mutate(across(everything(), as.character))

  deletion_log <- bind_rows(all_dlogs, cleaning_log_deletions) %>%
    bind_rows(all_dlogs) %>%
    bind_rows(manual_dlog) %>%
    distinct(uuid, .keep_all = T)


  # ──────────────────────────────────────────────────────────────────────────────
  # 3. Apply the clog to each item in the list of dfs and clogs
  # ──────────────────────────────────────────────────────────────────────────────

  ## first just review the clog against the raw data

  message("Reviewing clogs...")
  Sys.sleep(1)

  # clog_review <- purrr::map(list_of_df_and_clog, function(x) {
  #   clog_review <- cleaningtools::review_cleaning_log(
  #     raw_dataset = x$raw_data,
  #     raw_data_uuid_column = "uuid",
  #     cleaning_log = x$cleaning_log,
  #     cleaning_log_change_type_column = "change_type",
  #     change_response_value = "change_response",
  #     cleaning_log_question_column = "question",
  #     cleaning_log_uuid_column = "uuid",
  #     cleaning_log_new_value_column = "new_value")
  # })

  message("Updating clogs and recoding others")
  Sys.sleep(1)

  cleaning_log_summaries <- purrr::map(list_of_df_and_clog, function(x) {

    # process the clogs to update the others and remove items from the clogs
    cleaned_log <- x$cleaning_log %>%
      filter(!index %in% deletion_log$index) %>% ## needs to be the index not the UUID because index is the same across all rosters, where as UUID is unique to the roster
      mutate(new_value = as.character(new_value))

    temp_clog_type <- cleaned_log$clog_type[1]

    updated_log <- update_others(kobo_survey, kobo_choice, cleaning_log = cleaned_log) %>%
      mutate(clog_type = temp_clog_type)

    ## remove any dlogs from the raw data
    cleaned_data <- x$raw_data %>%
      filter(!index %in% deletion_log$index)

    ## easy list output
    list(
      pre_raw_data = x$raw_data, ##this is the data without the time deletion removed... the other one isnt really raw.
      cleaning_log = updated_log,
      raw_data = cleaned_data
    )
  })

  ### apply the cleaning log to each items in the list

  message("✅ Creating clean data...")
  Sys.sleep(2)

  clean_data_logs <- purrr::map(cleaning_log_summaries, function(x) {

    message(paste0("creating clean data for: ", x$cleaning_log$clog_type[1]))

    Sys.sleep(2)

    my_clean_data <- create_clean_data(raw_dataset = x$raw_data,
                                       raw_data_uuid_column = "uuid",
                                       cleaning_log = x$cleaning_log,
                                       cleaning_log_uuid_column = "uuid",
                                       cleaning_log_question_column = "question",
                                       cleaning_log_new_value_column = "new_value",
                                       cleaning_log_change_type_column = "change_type")



    message("✅ Successfully created clean data, filtering tool...")
    Sys.sleep(2)

    # Identify relevant select_multiple parent questions in the dataset -- this speeds it up massively
    relevant_sm_parents <- my_clean_data %>%
      names() %>%
      keep(~ str_detect(.x, "/")) %>%
      str_remove("/[^/]+$") %>%
      unique()

    # Filter kobo_survey for relevant select_multiple questions
    relevant_kobo_survey <- kobo_survey %>%
      filter(str_detect(type, "select_multiple")) %>%
      filter(name %in% relevant_sm_parents)

    # Extract relevant list_names and filter kobo_choices
    relevant_list_names <- relevant_kobo_survey %>%
      pull(type) %>%
      str_remove("select_multiple ") %>%
      unique()

    relevant_kobo_choices <- kobo_choice %>%
      filter(list_name %in% relevant_list_names)

    temp_clog_type <- x$cleaning_log$clog_type[1]

    if (temp_clog_type != "main") {

      processing_del_log <- x$pre_raw_data %>%
        select(index, uuid, contains("parent_instance"))

      deletion_log <- processing_del_log %>%
        filter(index %in% deletion_log$index)

    } else {

      deletion_log = deletion_log

    }


    if(nrow(relevant_kobo_survey != 0)) {

      message(paste0("✅ Successfully filtered tool, now making parent cols for: ", x$cleaning_log$clog_type[1]))
      Sys.sleep(2)


      my_clean_data_parentcol <- recreate_parent_column(dataset = my_clean_data,
                                                        uuid_column = "uuid",
                                                        kobo_survey = relevant_kobo_survey,
                                                        kobo_choices = relevant_kobo_choices,
                                                        sm_separator = "/",
                                                        cleaning_log_to_append = x$cleaning_log)

      message(paste0("✅ Processed: ", x$cleaning_log$clog_type[1]))
      Sys.sleep(0.5)

      return(list(
        raw_data = x$pre_raw_data %>% utils::type.convert(as.is = TRUE),
        my_clean_data_final = my_clean_data_parentcol$data_with_fix_concat,
        cleaning_log = my_clean_data_parentcol$cleaning_log,
        deletion_log = deletion_log
      ))

    } else {

      message(paste0("✅ No select multiple questions for ", x$cleaning_log$clog_type[1], " returning existing clog."))
      Sys.sleep(2)

      return(list(
        raw_data = x$pre_raw_data %>% utils::type.convert(as.is = TRUE),
        my_clean_data_final = my_clean_data,
        cleaning_log = x$cleaning_log,
        deletion_log = deletion_log
      ))

    }
  })


  ### combine the clog together

  all_cleaning_logs <- purrr::map(clean_data_logs, function(x){
    cleaning_log <- x$cleaning_log
  })

  all_cleaning_logs <- bind_rows(all_cleaning_logs)


} else {


  message("No cleaning logs provided... Making clean data just from deletion logs.")


  # Read in all the dlogs
  all_dlogs <- readxl::read_excel(r"(03_output/02_deletion_log/deletion_log.xlsx)", col_types = "text")
  #manual_dlog <- readxl::read_excel(r"(03_output/02_deletion_log/MSNA_2025_Manual_Deletion_Log.xlsx)", col_types = "text")
  deletion_log <- bind_rows(all_dlogs) %>%
    distinct(uuid, .keep_all = T)

  message("✅ Outputted clean Data")

  all_clean_data <- purrr::map(all_raw_data, function(x) {

    x %>%
      filter(! index %in% deletion_log$index)
  }
  )

  all_clean_data$main %>%
    writexl::write_xlsx(., "03_output/06_clean_data/final_clean_main_data.xlsx")

  all_clean_data %>%
    write_rds(., "03_output/06_clean_data/final_data.rds")

  message("✅ Outputted raw Data")


  all_raw_data$main %>%
    writexl::write_xlsx(., "03_output/01_raw_data/raw_data_main.xlsx")

  message("✅ Outputted deletion log")


  deletion_log %>%
    writexl::write_xlsx(., "03_output/02_deletion_log/combined_deletion_log.xlsx")
}

## now add the geospatial metadata to the output

clean_data_logs$main$my_clean_data_final <- clean_data_logs$main$my_clean_data_final %>%
  left_join(fo_district_mapping %>%  select(contains("admin"))) %>%
  relocate(admin_2_name, .after = "admin_2") %>%
  relocate(admin_1_name, .after = "admin_1") %>%
  left_join(point_data) %>%
  left_join(geo_ref_data %>% select(Hex_ID= hex_ID, unit_of_analysis)) %>%
  select(-hc_hex_id, -idp_hex_id)

main_to_join <- clean_data_logs$main$my_clean_data_final %>%
  dplyr::select(admin_1, admin_1_name, admin_2, admin_2_name,  admin_3, today,enum_id, resp_gender,
                hoh_gender,instance_name, index, Hex_ID, unit_of_analysis)

clean_data_logs <- imap(clean_data_logs, function(df, name) {
  if (name == "main") {
    df
  } else {
    df$my_clean_data_final <- df$my_clean_data_final %>%
      left_join(main_to_join, by = join_by(index == index))
    df
  }}
)

# ──────────────────────────────────────────────────────────────────────────────
# 3. Now review the cleaning and output the clog issues
# ──────────────────────────────────────────────────────────────────────────────

if(! is_empty(file_list)) {

  clog_issues <- purrr::map(clean_data_logs, function(x) {


    review_cleaning <- review_cleaning(x$raw_data,
                                       raw_dataset_uuid_column = "uuid",
                                       x$my_clean_data_final,
                                       clean_dataset_uuid_column = "uuid",
                                       cleaning_log = x$cleaning_log,
                                       cleaning_log_uuid_column = "uuid",
                                       cleaning_log_change_type_column = "change_type",
                                       cleaning_log_question_column = "question",
                                       cleaning_log_new_value_column = "new_value",
                                       cleaning_log_old_value_column = "old_value",
                                       cleaning_log_added_survey_value = "added_survey",
                                       cleaning_log_no_change_value = c("no_action", "no_change"),
                                       deletion_log = x$deletion_log,
                                       deletion_log_uuid_column = "uuid",
                                       check_for_deletion_log = T
    )

    review_cleaning <- review_cleaning %>%
      left_join(cleaning_logs %>% select(uuid, file_path) %>% distinct(), by = "uuid")

    review_cleaning = review_cleaning

  })


  writexl::write_xlsx(clog_issues, paste0("01_cleaning_logs/00_clog_review/cleaning_log_review_", lubridate::today(), ".xlsx"))


  ## now we will drop the LCIS indicators that are not being used

  lcsi_columns <- clean_data_logs$main$my_clean_data_final %>%
    select(matches("fsl_lcsi_stress|fsl_lcsi_crisis|fsl_lcsi_emergency")) %>%
    select(-check_fsl_lcsi_crisis5) %>%
    colnames()

  obs <- nrow(clean_data_logs$main$my_clean_data_final)

  lcsi_nas <- clean_data_logs$main$my_clean_data_final %>%
    select(uuid, any_of(lcsi_columns)) %>%
    pivot_longer(lcsi_columns) %>%
    filter(value == "not_applicable") %>%
    count(name) %>%
    arrange(desc(n)) %>%
    mutate(pct_not_applicable = n / obs)

    lcsi_stress <- lcsi_nas %>%
      filter(str_detect(name, "fsl_lcsi_stress")) %>%
      slice_min(pct_not_applicable, n = 4) %>%
      pull(name)

    lcsi_emergency <- lcsi_nas %>%
      filter(str_detect(name, "fsl_lcsi_emergency")) %>%
      slice_min(pct_not_applicable, n = 3) %>%
      pull(name)

    lcsi_crisis <- lcsi_nas %>%
      filter(str_detect(name, "fsl_lcsi_crisis")) %>%
      slice_min(pct_not_applicable, n = 3) %>%
      pull(name)

  ### add indicators to the cleaned main data

  clean_data_logs$main$my_clean_data_final <- clean_data_logs$main$my_clean_data_final %>%
    mutate(fsl_hhs_nofoodhh = ifelse(fsl_hhs_nofoodhh == "dnk" | fsl_hhs_nofoodhh == "pnta", NA, fsl_hhs_nofoodhh),
           fsl_hhs_alldaynight = ifelse(fsl_hhs_alldaynight == "dnk" | fsl_hhs_alldaynight == "pnta", NA, fsl_hhs_alldaynight),
           fsl_hhs_sleephungry = ifelse(fsl_hhs_sleephungry == "dnk" | fsl_hhs_sleephungry == "pnta", NA, fsl_hhs_sleephungry)) %>%
    add_eg_fcs(
      cutoffs = "normal") %>%
    add_eg_hhs(
      hhs_nofoodhh_1 = "fsl_hhs_nofoodhh",
      hhs_nofoodhh_1a = "fsl_hhs_nofoodhh_freq",
      hhs_sleephungry_2 = "fsl_hhs_sleephungry",
      hhs_sleephungry_2a = "fsl_hhs_sleephungry_freq",
      hhs_alldaynight_3 = "fsl_hhs_alldaynight",
      hhs_alldaynight_3a = "fsl_hhs_alldaynight_freq",
      yes_answer = "yes",
      no_answer = "no",
      rarely_answer = "rarely",
      sometimes_answer = "sometimes",
      often_answer = "often"
    ) %>%
    add_eg_lcsi(
      lcsi_stress_vars = lcsi_stress,
      lcsi_crisis_vars = lcsi_crisis,
      lcsi_emergency_vars = lcsi_emergency,
      yes_val = "yes",
      no_val = "no_had_no_need",
      exhausted_val = "no_exhausted",
      not_applicable_val = "not_applicable") %>%
    add_eg_rcsi(
      rCSILessQlty = "fsl_rcsi_lessquality",
      rCSIBorrow = "fsl_rcsi_borrow",
      rCSIMealSize = "fsl_rcsi_mealsize",
      rCSIMealAdult = "fsl_rcsi_mealadult",
      rCSIMealNb = "fsl_rcsi_mealnb",
      new_colname = "rcsi"
    ) %>%
    add_eg_fcm_phase(
      fcs_column_name = "fsl_fcs_cat",
      rcsi_column_name = "rcsi_cat",
      hhs_column_name = "hhs_cat_ipc",
      fcs_categories_acceptable = "Acceptable",
      fcs_categories_poor = "Poor",
      fcs_categories_borderline = "Borderline",
      rcsi_categories_low = "No to Low",
      rcsi_categories_medium = "Medium",
      rcsi_categories_high = "High",
      hhs_categories_none = "None",
      hhs_categories_little = "Little",
      hhs_categories_moderate = "Moderate",
      hhs_categories_severe = "Severe",
      hhs_categories_very_severe = "Very Severe"
    ) %>%
    impactR4PHU::add_hdds(
      fsl_hdds_cereals = "fsl_hdds_cereal",
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
      fsl_hdds_condiments = "fsl_hdds_condiments",
      yes_val = "yes",
      no_val = "no"
    )

#   ### add indicators to the nutrition section
#   clean_data_logs$child_feeding$my_clean_data_final <- clean_data_logs$child_feeding$my_clean_data_final %>%
#     mutate(drink_yoghurt_yn = ifelse(yoghurt > 0, "yes", "no")) %>%
#     impactR4PHU::add_iycf(
#                           yes_value = "yes",
#                           no_value = "no",
#                           dnk_value = "dnk",
#                           pna_value = "pnta",
#                           age_months = "child_age_months",
#                           iycf_1 = "breastfed_ever", # ever breastfed (y/n)
#                           iycf_2 = "child_first_breastfeeding", # how long the child started breastfeeding after birth
#                           iycf_4 = "breastfeeding", # breastfed yesterday during the day or night (y/n)
#                           iycf_5 = "infant_bottlefed", #indicates if the child drink anything from a bottle yesterday
#                           iycf_6a = "drink_water", # plain water
#                           iycf_6b = "drink_formula_yn", # infant formula (y/n)
#                           iycf_6c = "drink_milk_yn", # milk from animals, fresh tinned powder (y/n)
#                           iycf_6d = "drink_yoghurt_yn", # yoghurt drinks (y/n)
#                           iycf_6e = "chocolate_drink", # chocolate flavoured drinks, including from syrup / powders (y/n)
#                           iycf_6f = "juice_drink", # Fruit juice or fruit-flavoured drinks including those made from syrups or powders? (y/n)
#                           iycf_6g = "soda_drink", # sodas, malt drinks, sports and energy drinks (y/n)
#                           iycf_6h = "tea_drink", # tea, coffee, herbal drinks (y/n)
#                           iycf_6i = "broth_drink", # clear broth / soup (y/n)
#                           iycf_6j = "other_drink", # other liquids (y/n)
#                           iycf_7a = "yoghurt", # yoghurt (NOT yoghurt drinks) (number)
#                           iycf_7b = "cereals",
#                           iycf_7c = "vegetables", # vitamin a rich vegetables (pumpkin, carrots, sweet red peppers, squash or yellow/orange sweet potatoes) (y/n)
#                           iycf_7d = "root_vegetables", # white starches (plaintains, white potatoes, white yams, manioc, cassava) (y/n)
#                           iycf_7e = "leafy_vegetables", # dark green leafy vegetables (y/n)
#                           iycf_7f = "vegetables_other", # other vegetables (y/n)
#                           iycf_7g = "tropical_fruits", # vitamin a rich fruits (ripe mangoes, ripe papayas) (y/n)
#                           iycf_7h = "fruits_other", # other fruits (y/n)
#                           iycf_7i = "organ_meats", # organ meats (liver ,kidney, heart) (y/n)
#                           iycf_7j = "meat", # processed meats (sausages, hot dogs, ham, bacon, salami, canned meat) (y/n)
#                           #iycf_7k = "other_meat", # any other meats (beef, chicken, pork, goat, chicken, duck) (y/n)
#                           iycf_7l = "egg", # eggs (y/n)
#                           iycf_7m = "seafood", # fish (fresh or dried fish or shellfish) (y/n)
#                           iycf_7n = "legumes", # legumes (beans, peas, lentils, seeds, chickpeas) (y/n)
#                           iycf_7o = "cheese", # cheeses (hard or soft cheeses) (y/n)
#                           #iycf_7p = "sweet_food", # sweets (chocolates, candies, pastries, cakes) (y.n)
#                           #iycf_7q = "chips", # fried or empty carbs (chips, crisps, french fries, fried dough, instant noodles) (y/n)
#                           iycf_7r = "food_other",
#                           iycf_8 = "times_solid", # times child ate solid/semi-solid foods (number),
#                           uuid = "uuid") %>%
#     mutate(across(starts_with("other_"), as.numeric)) %>%
#     impactR4PHU::check_iycf_flags(.dataset = .,
#                                   age_months = "child_age_months",
#                                   iycf_4 = "breastfed_yesterday", # breastfed yesterday during the day or night (y/n)
#                                   iycf_6a = "drink_water", # plain water
#                                   iycf_6b = "drink_formula_yn", # infant formula (y/n)
#                                   iycf_6c = "drink_milk_yn", # milk from animals, fresh tinned powder (y/n)
#                                   iycf_6d = "drink_yoghurt_yn", # yoghurt drinks (y/n)
#                                   iycf_6e = "chocolate_drink", # chocolate flavoured drinks, including from syrup / powders (y/n)
#                                   iycf_6f = "juice_drink", # Fruit juice or fruit-flavoured drinks including those made from syrups or powders? (y/n)
#                                   iycf_6g = "soda_drink", # sodas, malt drinks, sports and energy drinks (y/n)
#                                   iycf_6h = "tea_drink", # tea, coffee, herbal drinks (y/n)
#                                   iycf_6i = "broth_drink", # clear broth / soup (y/n)
#                                   iycf_6j = "other_drink", # other liquids (y/n)
#                                   iycf_7a = "yoghurt_food", # yoghurt (NOT yoghurt drinks) (number)
#                                   iycf_7b = "porridge_food",
#                                   iycf_7c = "pumpkin_food", # vitamin a rich vegetables (pumpkin, carrots, sweet red peppers, squash or yellow/orange sweet potatoes) (y/n)
#                                   iycf_7d = "plantain_food", # white starches (plaintains, white potatoes, white yams, manioc, cassava) (y/n)
#                                   iycf_7e = "vegetables_food", # dark green leafy vegetables (y/n)
#                                   iycf_7f = "other_vegetables", # other vegetables (y/n)
#                                   iycf_7g = "fruits", # vitamin a rich fruits (ripe mangoes, ripe papayas) (y/n)
#                                   iycf_7h = "other_fruits", # other fruits (y/n)
#                                   iycf_7i = "liver", # organ meats (liver ,kidney, heart) (y/n)
#                                   iycf_7j = "canned_meat", # processed meats (sausages, hot dogs, ham, bacon, salami, canned meat) (y/n)
#                                   iycf_7k = "other_meat", # any other meats (beef, chicken, pork, goat, chicken, duck) (y/n)
#                                   iycf_7l = "eggs", # eggs (y/n)
#                                   iycf_7m = "fish", # fish (fresh or dried fish or shellfish) (y/n)
#                                   iycf_7n = "cereals", # legumes (beans, peas, lentils, seeds, chickpeas) (y/n)
#                                   iycf_7o = "cheese", # cheeses (hard or soft cheeses) (y/n)
#                                   iycf_7p = "sweet_food", # sweets (chocolates, candies, pastries, cakes) (y.n)
#                                   iycf_7q = "chips", # fried or empty carbs (chips, crisps, french fries, fried dough, instant noodles) (y/n)
#                                   iycf_7r = "other_solid",
#                                   iycf_8 = "times_solid",
#                                   iycf_6b_num = "drink_formula",
#                                   iycf_6c_num = "drink_milk",
#                                   iycf_6d_num = "drink_yoghurt")

  # ──────────────────────────────────────────────────────────────────────────────
  # 4. Output everything
  # ──────────────────────────────────────────────────────────────────────────────

  all_cleaning_logs %>%
    filter(question != "index") %>%
    writexl::write_xlsx(., "03_output/06_clean_data/final_agg_cleaning_log.xlsx")

  clean_data_logs %>%
    write_rds(., "03_output/06_clean_data/final_all_r_object.rds")

  clean_data_logs$main$my_clean_data_final %>%
    writexl::write_xlsx(., "03_output/06_clean_data/final_clean_main_data.xlsx")

  clean_data_logs$main$raw_data %>%
    writexl::write_xlsx(., "03_output/01_raw_data/raw_data_main.xlsx")

  deletion_log %>%
    writexl::write_xlsx(., "03_output/02_deletion_log/combined_deletion_log.xlsx")

}



# ──────────────────────────────────────────────────────────────────────────────
# 5. Review the HH_size vs UoA average
# ──────────────────────────────────────────────────────────────────────────────

admin_lookup <- geo_ref_data %>% select(admin_2, district, unit_of_analysis) %>% distinct() %>%
  mutate(admin_3 = ifelse(str_detect(unit_of_analysis, "IDP"), "idp_sites", "hc"))

UoA_hh_size <- clean_data_logs$main$my_clean_data_final %>%
  left_join(admin_lookup) %>%
  group_by(unit_of_analysis) %>%
  summarise(avg_UoA = mean(hh_size, na.rm = T),
            count_region = n())

enum_hh_size <- clean_data_logs$main$my_clean_data_final %>%
  left_join(admin_lookup) %>%
  group_by(unit_of_analysis, enum_id) %>%
  summarise(avg_enum = mean(hh_size, na.rm = T),
            surveys_done = n()) %>%
  ungroup() %>%
  filter(surveys_done > 3)


hh_ratio <- enum_hh_size %>%
  left_join(UoA_hh_size, by = join_by("unit_of_analysis")) %>%
  mutate(hh_ratio = avg_enum / avg_UoA) %>%
  filter(hh_ratio < 0.66)

clean_data_logs$main$my_clean_data_final %>%
  left_join(admin_lookup) %>%
  group_by(unit_of_analysis) %>%
  summarise(avg_HH_size = mean(hh_size), number_interviews = n())

enum_hh_size %>%
  filter(str_detect(unit_of_analysis, "Bay Urban IDP")) %>%
  arrange(avg_enum)


hh_ratio %>%
  writexl::write_xlsx(., "03_output/08_hh_size_check/hh_size_check.xlsx")


# ──────────────────────────────────────────────────────────────────────────────
# 5. Review the LCSI scores
# ──────────────────────────────────────────────────────────────────────────────

lcsi_uuids <- clean_data_logs$main$my_clean_data_final %>% filter(fsl_fcs_category == "Poor" &
                                if_all(
                                  c(
                                    fsl_lcsi_crisis1,
                                    fsl_lcsi_crisis2,
                                    fsl_lcsi_crisis3,fsl_lcsi_crisis4, fsl_lcsi_crisis5,
                                    fsl_lcsi_emergency1,
                                    fsl_lcsi_emergency2,
                                    fsl_lcsi_emergency3,fsl_lcsi_emergency4, fsl_lcsi_emergency5,
                                    fsl_lcsi_stress1,
                                    fsl_lcsi_stress2,
                                    fsl_lcsi_stress3,
                                    fsl_lcsi_stress4, fsl_lcsi_stress5, fsl_lcsi_stress6
                                  ),
                                  ~ .x == "no_had_no_need"
                                )) %>% pull(`_id`)



raw_metadata_length <- read_rds("03_output/01_raw_data/raw_metadata.rds")
fo_district_mapping <- read_excel("02_input/04_fo_input/fo_base_assignment_MSNA_25.xlsx") %>%
  rename(fo_in_charge = FO_In_Charge)

avg_time <- raw_metadata_length %>% mutate(metadata_duration_seconds = end - start) %>%
  filter(str_detect(name, "lcsi")) %>% group_by(name) %>% summarise(average_time = mean(metadata_duration, na.rm = T))

problem_questions <- raw_metadata_length %>% mutate(duration_seconds = end - start) %>% filter(`_id` %in%  lcsi_uuids) %>%
  filter(str_detect(name, "lcsi")) %>% select(`_id`, name, `new-value`, duration_seconds) %>%
  left_join(all_raw_data$main %>% select(`_id`, enum_id, admin_2)) %>%
  left_join(fo_district_mapping %>% select(admin_2, admin_2_name, fo_in_charge)) %>%
  select(-admin_2) %>%
  filter(! str_detect(name, "note")) %>%
  group_by(enum_id) %>%
  summarise(avg_time_spent = median(duration_seconds))




# ──────────────────────────────────────────────────────────────────────────────
# 5. Review the soft duplicates
# ──────────────────────────────────────────────────────────────────────────────

## read in already approved ones
exclusions <- read_excel("02_input/02_duplicate_exclusions/exclusions.xlsx")

my_clean_data_added <- clean_data_logs$main$my_clean_data_final %>%
  left_join(fo_district_mapping) %>%
  rename(fo =fo_in_charge)

enum_typos <- my_clean_data_added %>%
  dplyr::count(enum_id) %>%
  filter(n >= 5) %>%
  pull(enum_id)

group_by_enum <- my_clean_data_added %>%
  filter(enum_id %in% enum_typos) %>%
  group_by(enum_id)

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


if(nrow(similar_surveys) > 0) {
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
} else {
  message("No duplicate surveys detected")

}

# ──────────────────────────────────────────────────────────────────────────────
# 6. Review FCS Scores
# ──────────────────────────────────────────────────────────────────────────────

clean_data_logs$main$my_clean_data_final %>%
  mutate(fsl_fcs_score = as.double(fsl_fcs_score)) %>%
  ggplot(aes(fsl_fcs_score)) +
  geom_histogram() +
  geom_vline(xintercept =  70, colour = "orangered3", linetype = "dashed", linewidth = 0.5) +
  geom_vline(xintercept =  10, colour = "orangered3", linetype = "dashed", linewidth = 0.5) +
  facet_wrap(~ admin_3)

clean_data_logs$main$my_clean_data_final %>%
  mutate(fsl_fcs_score = as.double(fsl_fcs_score)) %>%
  group_by(admin_3) %>%
  summarise(avg_fcs = mean(fsl_fcs_score, na.rm = T))

clean_data_logs$main$my_clean_data_final %>%
  mutate(fsl_fcs_score = as.double(fsl_fcs_score)) %>%
  ggplot(., aes(x= admin_3, y = fsl_fcs_score)) +
  geom_boxplot(fill="slateblue", alpha=0.2) +
  geom_jitter(color="black", size=0.2, alpha=0.5) +
  xlab("")




# ──────────────────────────────────────────────────────────────────────────────
# 6. Review MUAC data
# ──────────────────────────────────────────────────────────────────────────────

count_surveys <- clean_data_logs$main$my_clean_data_final %>%
  count(unit_of_analysis) %>%
  rename(total_surveys = n)

clean_data_logs$nut_ind$my_clean_data_final %>%
  left_join(main_to_join) %>%
  filter(!is.na(muac_cm)) %>%
  count(unit_of_analysis) %>%
  left_join(count_surveys) %>%
  select(unit_of_analysis, muac_surveys = n, total_surveys) %>%
  mutate(muac_per_survey = muac_surveys / total_surveys) %>%
  left_join(geo_ref_data %>% group_by(unit_of_analysis) %>% summarise(target_sample_size = sum(survey_buffer))) %>%
  mutate(pct_complete = total_surveys / target_sample_size) %>%
  rename(surveys_done = total_surveys) %>%
  writexl::write_xlsx(., "03_output/muac_checks.xlsx")

clean_data_logs$roster$my_clean_data_final %>%
  left_join(main_to_join) %>%
  mutate(under_5 = ifelse(ind_age < 5, TRUE, FALSE)) %>%
  group_by(unit_of_analysis) %>%
  summarise(pct_under_5 = mean(under_5),
            srvy_count = n())

### Cabudwaaq collected

collected_check <- clean_data_logs$main$my_clean_data_final %>%
  mutate(collected = "YES") %>%
  left_join(fo_district_mapping) %>%
  distinct(point_id, .keep_all = T)

read_csv("04_tool/sample_points.csv") %>%
  distinct() %>%
  select(District, Hex_ID, point_id = Point_ID) %>%
  left_join(collected_check %>% select(District = admin_2_name, point_id, collected), by = join_by("point_id", "District")) %>%
  mutate(collected = ifelse(is.na(collected), "NO", collected)) %>%
  writexl::write_xlsx("all_points_confirmed.xlsx")


points_check <- read_csv("04_tool/sample_points.csv") %>%
  distinct() %>%
  select(District, Hex_ID, point_id = Point_ID) %>%
  left_join(collected_check %>% select(District = admin_2_name, point_id, collected), by = join_by("point_id", "District")) %>%
  mutate(collected = ifelse(is.na(collected), "NO", collected))

points_check %>%
  filter(Hex_ID %in% c('H201335', 'H201336', 'H201374')) %>%
  View()


clean_data_logs$nut_ind$my_clean_data_final %>%
  filter(!is.na(oedema_confirm)) %>%
  View()



## output for Saeed
clean_data_logs$main$my_clean_data_final <- clean_data_logs$main$my_clean_data_final %>%
  select(-matches("_gps|geopoint|pt_num|pt_sample|point_number|point_number|threshold_msg|reasons_why_far|final_text"))

clean_data_logs$main$raw_data <- clean_data_logs$main$raw_data %>%
  select(-matches("_gps|geopoint|pt_num|pt_sample|point_number|point_number|threshold_msg|reasons_why_far|final_text"))

clean_data_logs %>%
  write_rds(., "SOM_MSNA_P1_all_data_no_PII.rds")


list(HH_data <- clean_data_logs$main$my_clean_data_final, roster = clean_data_logs$roster$my_clean_data_final, mortality = clean_data_logs$$my_clean_data_final)


flat_list <- imap(clean_data_output, function(sublist, clean_data_output) {
  imap(sublist, function(df, child_name) {
    df  # Just return the dataframe
  }) %>%
    set_names(~ paste(clean_data_output, ., sep = "_"))  # Prefix child names
}) %>%
  flatten()
