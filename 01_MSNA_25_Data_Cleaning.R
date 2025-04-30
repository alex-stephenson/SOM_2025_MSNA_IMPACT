### PROJECT:  MSNA
### PURPOSE:  Data Checks MSNA 2024
### INPUT:    xlsx file
### OUTPUT:   csv file
### AUTHOR:   Aaron Kipkemoi
### LAST UPDATED: 1st June, 2024
## Data Cleaning Script
rm(list = ls())

# set the working directory
setwd('C:/Users/aaron.langat/Documents/R/03_MSNA/01_Data_Cleaning')



###############Load functions###################################
source("01_inputs/03_Functions/cleaning_functions.R")

# get the timestamp to more easily keep track of different versions
date_time_now <- format(Sys.time(), "%b_%d_%Y_%H%M%S")
today <- Sys.Date()

if (!require("pacman")) install.packages("pacman")
p_load(rio,
       tidyverse,
       koboquest,
       crayon,
       hypegrammaR,
       sjmisc)
# load packages
library(cleaningtools)
library(dplyr)
library(readxl)
library(openxlsx)
library(stringr)
library(purrr)
library(naniar)
library(hypegrammaR)
library(httr)
library(KoboconnectR)
library(lubridate)
library(geosphere)
library(plyr)
library(healthyr)
library(impactR4PHU)

##################################################################################################
####################Download data from kobo#############################
###################################################################################################
# update this based on your details / project
# kobo_user_name <- "som_data"
# kobo_pw <- "P@ssw0rd"
# kobo_url <- "kc.impact-initiatives.org"
# kobo_project_name <- "2024_MSNA_Tool_testing"
#
# # get token - make sure your API key is set up in the Security section of Account Settings on Kobo Online
# get_kobo_token(url = "kc.impact-initiatives.org", uname = "som_data", pwd = "2024_MSNA_Tool_testing")
#
# # get the list of all of my kobo projects
# kobo_projects <- as.data.frame(kobotools_api(url = ,
#                                              simplified = T,
#                                              uname = kobo_user_name,
#                                              pwd = kobo_pw)
# )
#
# # get the asset id of the kobo project for the HSM
# kobo_project_asset_id <- kobo_projects %>%
#   filter(name == kobo_project_name) %>%
#   pull(asset)
#
# # create a new export
# # only run this when new data is entered and you want a new export with this data
# new_export_url <- kobo_export_create(url = kobo_url,
#                                      uname = kobo_user_name,
#                                      pwd = kobo_pw,
#                                      assetid = kobo_project_asset_id,
#                                      type = "xls",
#                                      all = "false",
#                                      lang = "_xml",
#                                      hierarchy = "false",
#                                      include_grp = "false",
#                                      grp_sep = "/",
#                                      multi_sel = "both",
#                                      media_url = "false",
#                                      sleep = 25
# )
#
# # get the list of existing exports - may need to delete some online if you run out of memory but this hasn't caused me any issues to date
# existing_exports <- as.data.frame(kobo_exports(url = kobo_url,
#                                                uname = kobo_user_name,
#                                                pwd = kobo_pw)$results
# )
#
# # filter for the asset id, arrange by the date/time the export was created
# existing_exports_formatted <- existing_exports %>%
#   select(date_created, last_submission_time, result) %>%
#   bind_cols(existing_exports$data) %>%
#   filter(grepl(kobo_project_asset_id, source)) %>%
#   arrange(desc(date_created))
#
# # get the most recent export created, assuming that's what you want!!!
# project_most_recent_export_url <- existing_exports_formatted[1,]$result
#
# # download the data from the URL
# kobo_project_export <- httr::GET(project_most_recent_export_url, # if you just created a new export url it should show up here after you pull the existing ones, but just make sure!
#                                  httr::authenticate(user = kobo_user_name,
#                                                     password = kobo_pw)
# )
#
# kobo_project_content <- httr::content(kobo_project_export,
#                                       type = "raw",
#                                       encoding = "UTF-8")
#
#
# # write the raw data and read it back in for cleaning
# kobo_data_export_path <- paste0("01_inputs/02_Data/MSNA_2023", date_time_now, ".xlsx")
# writeBin(kobo_project_content, kobo_data_export_path)
# raw_data <- read_excel(kobo_data_export_path)#####Read main data

############################################################Read all datasets to R platform##############################################################
df = "01_inputs/02_Data/SOM2404_MSNA_Tool.xlsx"

raw_data <- read.xlsx(df,sheet = "SOM2404_MSNA_Tool")

main_data <- raw_data

###################Assign raw data to the df for futher data processing

#blank dnk and pnta



###################define initiatlized variables
consent <- "consent"

uuid <- "uuid"
mindur <- 40
maxdur <- 90
surveydate <- "today"
#####################################################################################
#####################################################################################
################Convert  all columns to respective data type format
#####################################################################################
#####################################################################################
##################read the tool######
koboToolPath = "01_inputs/01_tool/REACH_2024_MSNA_kobo tool.xlsx"
questions = import(koboToolPath,sheet="survey") %>% filter(!is.na(name))
choices = import(koboToolPath,sheet="choices")

fo_bases <- read_excel("01_inputs/05_FO_Bases/FO_In_Charge.xlsx")#################read fo bases
main_data <- main_data %>% left_join(fo_bases, by = "admin_2")########join fo names with


tool.survey<- questions%>%filter(name%in%colnames(main_data))
#convert columns to respective data types

# # convert integers columns to numeric
cols.integer <- filter(tool.survey, type=="integer")$name
main_data <- mutate_at(main_data, cols.integer, as.numeric)


cols.gps <- filter(tool.survey, type=="gps")$name
main_data <- mutate_at(main_data, cols.gps, as.integer)

#############convert gps columns to integer
columns_to_convert <- c("_household_geopoint_latitude", "_household_geopoint_longitude")
for (col in columns_to_convert) {
  main_data[[col]] <- as.numeric(main_data[[col]])
}

# cols.integer <- filter(tool.survey, type=="calculate")$name
# df <- mutate_at(df, cols.integer, as.integer)
#
# #convert select_one to character
cols.characters <- filter(tool.survey,str_detect(type, "select_one"))$name
main_data <- mutate_at(main_data, cols.characters, as.character)


names(main_data)[names(main_data) == "_uuid"] <- "uuid" # fix uuid


## Check 1: Add time checks to the data
main_data <- time_check(main_data, time_min = mindur, time_max = maxdur)


to_delete <- main_data %>% filter(consent=="no" | interview_duration<40) %>% select(uuid)###To be deleted from every loop checks

####################negate function#######################
'%!in%' <- function(x,y)!('%in%'(x,y))

################################################Combine idp and settlement name columns to get one###########
#################################and add gender of the hoH####################################
############
main_data <- main_data %>%mutate(settlement_idp=  case_when(
  admin_3 == "settlement" ~ settlement,
  admin_3 == "idp_sites" ~ idp_site,
  TRUE ~ NA
),

final_hoh_gender=  case_when(
  resp_hoh_yn == "yes" ~ resp_gender,
  resp_hoh_yn == "no" ~ hoh_gender,
  TRUE ~ NA

))

#########################read all settlements
master_settlement <- read_excel("01_inputs/04_Sample/Master_List.xlsx")
main_data <- main_data %>% left_join(master_settlement, by ="settlement_idp")###join sample name with main data

#########Calculate distance difference between the sample and collected point
main_data <- main_data %>%
  mutate(
    dist_diff = geosphere::distHaversine(cbind(`_household_geopoint_longitude`, `_household_geopoint_latitude`),
                                         cbind(Longitude_samp, Latitude_samp))
  )

############################################################################################################
############################################################################################################
#########################Export Gis Data###########################################
###########################################################################################################
gis_data <- main_data %>% filter(interview_duration >= 40 & consent=="yes") %>%
  select(uuid,admin_1,admin_2,admin_3,settlement_idp,Name,contains("_household_geopoint"),enum_id,Longitude_samp,Latitude_samp,
         dist_diff,FO_In_Charge,today,Pcode) %>% dplyr::rename(adm1_rep= admin_1, adm2_rep= admin_2, adm3_rep= admin_3, pcode_rep = settlement_idp,
                                                               name_rep= Name, hh_lat = `_household_geopoint_latitude`, hh_long = `_household_geopoint_longitude`,
                                                               hh_alt = `_household_geopoint_altitude`, hh_prec = `_household_geopoint_precision`,
                                                               samp_long = Longitude_samp, samp_lat = Latitude_samp, FO_resp = FO_In_Charge,date= today)



previous <- read.csv("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/05_GIS/02_Spatial_Checks/for_checks.csv")

new <- filter(gis_data, uuid %!in% previous$uuid)#removing already checked ones

new <- new %>% mutate(date= format.Date(Sys.Date(), "%m%d%Y"))

gis_data <- rbind(new,previous)

###############Export master gis to gis folder data
write.csv(gis_data,"C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/05_GIS/02_Spatial_Checks/for_checks.csv", row.names = FALSE)

###################################################Export data for dashboard################################################
######################impor sample target to target calculation####################################################
##########################################################################################################################################
sample <- read.csv("01_inputs/04_Sample/Samples.csv")

################################################################################
dashboard_data <- main_data

#############################Clean  population status that might have been wrongly selected########################
###############################create clean data from Charity#####################
clog_input <- read.csv("01_inputs/02_Data/clog_final.csv")

dashboard_data <- create_clean_data(
  dashboard_data,
  raw_data_uuid_column = "uuid",
  clog_input,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column="question",
  cleaning_log_new_value_column="new_value",
  cleaning_log_change_type_column="change_type",
  change_response_value = "change_response",
  NA_response_value = "blank_response",
  no_change_value = "no_action",
  remove_survey_value = "remove_survey"
)

# Rename column where names is "Sepal.Length"
names(dashboard_data)[names(dashboard_data) == "admin_1"] <- "region"
names(dashboard_data)[names(dashboard_data) == "admin_2"] <- "district"


dashboard_data <- dashboard_data %>% filter(consent=="yes") %>%  mutate(district_pop=ifelse(place_of_origin=="yes", paste0(district,"_hc"),
                                                                                            ifelse(place_of_origin == "no" & time_of_movement== "less_one_year",paste0(district,"_new_idp"),
                                                                                                   ifelse(place_of_origin == "no" & time_of_movement== "more_one_year",paste0(district,"_protracted"),NA))),
                                                                        pop_status=ifelse(place_of_origin=="yes", paste0("Host Community"),
                                                                                          ifelse(place_of_origin == "no" & time_of_movement== "less_one_year",paste0("New IDP"),
                                                                                                 ifelse(place_of_origin == "no" & time_of_movement== "more_one_year",paste0("Protracted"),NA))),
                                                                        Deleted= ifelse(interview_duration<40,paste0(1),0),
                                                                        Retained= ifelse(interview_duration>=40,paste0(1),0)) %>% select(uuid,region,district,admin_3,interview_duration,
                                                                                                                                         settlement_idp,resp_gender,contains("_household_geopoint"),
                                                                                                                                         Deleted,Retained,district_pop,pop_status,enum_id,FO_In_Charge) %>%
  dplyr::rename(`Respondent Gender` = resp_gender, idp_settlement = admin_3,`_Gps Latitude` = `_household_geopoint_latitude`, `_Gps Longitude` = `_household_geopoint_longitude`, `_gps2_altitude` =	`_household_geopoint_altitude`,
                `_GpsPprecision` = `_household_geopoint_precision`
  )

############rename row names
dashboard_data <- dashboard_data %>% mutate(district=case_when(
  district == "aden_yabaal" ~ "Aden Yabaal",  district == "afgooye" ~ "Afgooye", district == "afmadow" ~ "Afmadow",
  district == "badhaadhe" ~ "Badhaadhe",  district == "baki" ~ "Baki", district == "balcad" ~ "Balcad",
  district == "banadir" ~ "Banadir",   district == "baardheere" ~ "Baardheere",
  district == "baraawe" ~ "Baraawe", district == "baydhaba" ~ "Baydhaba",district == "belet_weyne" ~ "Belet Weyne",
  district == "belet_xaawo" ~ "Belet Xaawo", district == "berbera" ~ "Berbera",district == "borama" ~ "Borama",
  district == "bossaso" ~ "Bossaso",
  district == "bulo_burto" ~ "Bulo Burto", district == "burco" ~ "Burco",district == "burtinle" ~ "Burtinle",
  district == "buuhoodle" ~ "Buuhoodle", district == "buur_hakaba" ~ "Buur Hakaba",district == "cadale" ~ "Cadale",
  district == "caynabo" ~ "Caynabo", district == "ceel_afweyn" ~ "Ceel Afweyn",district == "ceel_barde" ~ "Ceel Barde",
  district == "ceel_waaq" ~ "Ceel Waaq", district == "ceerigaabo" ~ "Ceerigaabo",district == "diinsoor" ~ "Diinsoor",
  district == "doolow" ~ "Doolow", district == "eyl" ~ "Eyl",district == "gaalkacyo" ~ "Gaalkacyo",
  district == "galdogob" ~ "Galdogob", district == "garbahaarey" ~ "Garbahaarey",district == "garoowe" ~ "Garowe",
  district == "gebiley" ~ "Gebiley", district == "hargeysa" ~ "Hargeysa",district == "iskushuban" ~ "Iskushuban",
  district == "jalalaqsi" ~ "Jalalaqsi", district == "jamaame" ~ "Jamaame",district == "jariiban" ~ "Jariiban",
  district == "jowhar" ~ "Jowhar", district == "kismayo" ~ "Kismayo",district == "laasqoray" ~ "Laasqoray",
  district == "lughaya" ~ "Lughaya", district == "luuq" ~ "Luuq",district == "marka" ~ "Marka",
  district == "owdweyne" ~ "Owdweyne", district == "qandala" ~ "Qandala",district == "qansax_dheere" ~ "Qansax Dheere",
  district == "qardho" ~ "Qardho", district == "qoryooley" ~ "Qoryooley",district == "sheikh" ~ "Sheikh",
  district == "taleex" ~ "Taleex", district == "waajid" ~ "Waajid",district == "wanla_weyn" ~ "Wanla_weyn",
  district == "xudun" ~ "Xudun", district == "xudur" ~ "Xudur",district == "zeylac" ~ "Zeylac",
  TRUE ~ district
)
)

dashboard_data <- dashboard_data %>% left_join(sample, by = "uuid")
#################################################Export dashboard data#####################################
write.xlsx(dashboard_data,"C:/Users/aaron.langat/Documents/My Tableau Repository/MSNA Data/dashboard_data.xlsx")


##############################################################################################
##############################################################################################
###################Data Checks Begin here#####################
##############################################################################################
##############################################################################################

##########################Check_main_data######################################################
##############################################################################################


main_data[is.na(main_data)] <- "" #fix all NA imports

checked_time <- main_data %>%

  # run the duplicate unique identifier check
  ########################################################################
check_logical(uuid_column = "uuid",
              check_id = "time_short",
              check_to_perform = "CHECK_interview_duration == \"Too short\" ",
              columns_to_clean = "interview_duration",
              description = "This survey will be deleted, because time duration is too short"
) %>%
  check_logical(uuid_column = "uuid",
                check_id = "time_long",
                check_to_perform = "CHECK_interview_duration == \"Too long\" ",
                columns_to_clean = "interview_duration",
                description = "Kindly inform this enumerator to keep an eye on his time management,
                the surveys are too long"
  )


##################ad them to cleaning log
checked_time_cleaning_log <-  checked_time %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )

####remove those surveys that will be deletd before I run the other checks
main_data_time <- main_data %>% filter(interview_duration >= 0 | consent=="no")


# we should exclude all questions from outlier checks that aren't integer response types (integer is the only numerical response type)
excluded_questions <- questions %>%
  filter(type != "integer") %>%
  pull(name) %>%
  unique()

# intersect
excluded_questions_in_data <- intersect(colnames(main_data_time), excluded_questions)

########################################################################################################################
#############################logical checks list in the main data######################################################
##############logic list, check list to check)
main_check_list <- data.frame(
  name = c("check migrate", "check left","check join","check more_death"),
  check = c(
    "household_plan == \"return_area_origin\" &  place_of_origin == \"yes\"",
    "num_left > 5",
    "num_join > 5",
    "num_died > 1"

  ),
  description = c(
    "logical\\Respondent mentioned to be originally from this location but selected that he plans to Return to area of origin (for IDP HHs only) ",
    "Number of people who left the household are more that 5 which is unlikely to happen even though in some cases it can be ",
    "Number of people who joined the household are more that 5 which is unlikely to happen even though in some cases it can be ",
    "Multiple deaths in a single HH which is unlikely"
  ),
  columns_to_clean = c("household_plan, place_of_origin",
                       "num_left",
                       "num_join",
                       "num_died"
  )
)

#################################################################################################################
#################################Continue with the rest of the checks############################
checked_main_data <-  main_data_time %>%

  # run the duplicate unique identifer check
  check_duplicate(
    uuid_column = "uuid",
    columns_to_check = NULL
  )  %>%

  # check for PII to be deleted later
  # check_pii(
  #     element_name = "checked_dataset",
  #     uuid_column = "uuid"
  # ) %>%

  # check if there are responses for the parts of the survey that allowed "other" responses
  # these will be recoded into selectable options during data cleaning
  # there are some inconsistencies in the "other" questions - we only need to check "_other" and not "/other"
  # "/other" is just the binary yes/no but "_other" is the actual text response
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(
      main_data_time |>
        dplyr::select(ends_with("_other")) |>
        dplyr::select(-contains("."))
    )

  ) %>%
  # Check for "I don't know" responses in numerical questions
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  ) %>%

  # conduct the logic checks
  # check_logical_with_list(  uuid_column = "uuid",
  #                           list_of_check = check_list,
  #                           check_id_column = "name",
  #                           check_to_perform_column = "check",
  #                           columns_to_clean_column = "columns_to_clean",
  #                           description_column = "description"
  # ) %>%

  # check for outliers
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
    columns_not_to_check = c(excluded_questions_in_data,"enum_id","enum_age", "interview_duration", "CHECK_interview_duration",
                             "_household_geopoint_latitude","_household_no_access_geopoint_latitude","_household_geopoint_altitude",
                             "_household_no_access_geopoint_altitude","_household_no_access_geopoint_precision","_household_geopoint_precision",
                             "Longitude_samp","Latitude_samp","_id","dist_diff", "fsl_fcs_cereals",
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


#######################potential_outliers##########################################################################################
#############################combine all cleaning logs for main dataset##########################
##########################################################################################
main_cleaning_log <-  checked_main_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )

## ADDED BY ABRAHAM
main_to_join <- main_data %>%
  dplyr::select(admin_1,admin_2,today,enum_id,resp_gender,
                hoh_gender,settlement_idp,FO_In_Charge,Name,deviceid,instance_name)
##########################################################################################################
##################################HH Roaster data cleaning is here########################
########################################################################################################
###prepare data for HH Roaster
hh_roster <- read_excel(df,sheet = "roster") %>% unique()#####import hh roster

hh_roster_bad_removed <- hh_roster %>%
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("parent_instance_name" = "instance_name"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::rename("uuid" = person_id) ## ADDED BY ABRAHAM
# hh_roster_bad_removed$admin_1 <- main_data$admin_1[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#region
# hh_roster_bad_removed$admin_2 <- main_data$admin_2[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#district
# hh_roster_bad_removed$today <- main_data$today[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#today
# hh_roster_bad_removed$enum_id <- main_data$enum_id[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#enum_name
# hh_roster_bad_removed$resp_gender <- main_data$resp_gender[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#resp_gender
# hh_roster_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#hoh gender
# hh_roster_bad_removed$settlement_idp <- main_data$settlement_idp[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#hoh gender
# hh_roster_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#Fo name and base
# hh_roster_bad_removed$Name <- main_data$Name[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#Name of the settlement
# hh_roster_bad_removed$deviceid <- main_data$deviceid[match(hh_roster_bad_removed$parent_instance_name, main_data$instance_name)]#Name of the deviceid

# names(hh_roster_bad_removed)[names(hh_roster_bad_removed) == "person_id"] <- "uuid" # fix uuid

checked_roster_data <- hh_roster_bad_removed %>%
  check_duplicate(
    uuid_column = "uuid",

  ) %>% check_logical(uuid_column = "uuid",
                      check_id = "logic_equal",
                      check_to_perform = "ind_age != calc_final_age_years",
                      columns_to_clean = "ind_age, calc_final_age_years",
                      description = "The keyed in age in years, is not the same as the calculated years(Usin the event calendar)"
  ) %>% check_logical(
    uuid_column = "uuid",
    check_to_perform = "ind_age_months > 60 & ind_age <= 5",
    check_id = "logic_above71",
    columns_to_clean = "final_ind_dob, ind_age",
    description =  "You indicated that this child is 5 years or less, but you have selected date of birth that is older than 5 years"
  )


roster_cleaning_log <-  checked_roster_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )



##########################################################################################################
#################################wgs cleaning is here########################
########################################################################################################
###prepare data for HH Roaster
wgs <- read_excel(df,sheet = "wgs_repeat")%>% distinct() #####import hh roster


wgs_bad_removed <- wgs %>% #remove surveys that will be deleted
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("parent_instance_name_001" = "instance_name"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::rename("uuid" = wgs_id) ## ADDED BY ABRAHAM #


# wgs_bad_removed$admin_1 <- main_data$admin_1[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#region
# wgs_bad_removed$admin_2 <- main_data$admin_2[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#district
# wgs_bad_removed$today <- main_data$today[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#today
# wgs_bad_removed$enum_id <- main_data$enum_id[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#enum_name
# wgs_bad_removed$resp_gender <- main_data$resp_gender[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#resp_gender
# wgs_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#hoh gender
# wgs_bad_removed$settlement_idp <- main_data$settlement_idp[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#hoh gender
# wgs_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#Fo name and base
# wgs_bad_removed$Name <- main_data$Name[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#Name of the settlement
# wgs_bad_removed$deviceid <- main_data$deviceid[match(wgs_bad_removed$parent_instance_name_001, main_data$instance_name)]#Name of the deviceid
#
# names(wgs_bad_removed)[names(wgs_bad_removed) == "wgs_id"] <- "uuid" # fix uuid


#####add number of disabilities

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
    ),

  )

wgs_bad_removed <- wgs_bad_removed %>%
  mutate( number_of_disabilities = rowSums(select(., calc_wgq_vision, calc_wgq_hearing, calc_wgq_mobility, calc_wgq_cognition,
                                                  calc_wgq_self_care, calc_wgq_communication), na.rm=T)
  )



checked_wgs_data <- wgs_bad_removed %>%
  check_logical(uuid_column = "uuid",
                check_id = "logic_dis",
                check_to_perform = "number_of_disabilities > 2",
                columns_to_clean = "wgq_vision,	wgq_hearing,	wgq_mobility,	wgq_cognition,	wgq_self_care,	wgq_communication",
                description = "This person has more than2 disabilities, unlikely to happen, please confirm"
  )


wgs_cleaning_log <-  checked_wgs_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )



##########################################################################################################
##################################HH Education data cleaning is here########################
########################################################################################################
###prepare data for Education data

###############################################################################################################################
#########################################logical list education################################################################
###############################################################################################################################

edu_check_list <- data.frame(
  name = c("check level"),
  check = c(
    "edu_ind_age  < 12 &  (edu_level_grade == \"level_three_diploma\" | edu_level_grade == \"bachelor\" |
    edu_level_grade == \"higher_education\" | edu_level_grade == \"level_three_masters\")"
  ),
  description = c(
    "logical\\This age is too young for the child to have that level of education"

  ),
  columns_to_clean = c("edu_level_grade, edu_ind_age")
)

#############################################################################

edu_roster <- read_excel(df,sheet = "edu_ind")#####import education roster

edu_roster_bad_removed <- edu_roster %>% #remove surveys that will be deleted
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("_submission__uuid" = "uuid"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::rename("uuid" = edu_person_id) ## ADDED BY ABRAHAM
#
# edu_roster_bad_removed$admin_1 <- main_data$admin_1[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#region
# edu_roster_bad_removed$admin_2 <- main_data$admin_2[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#district
# edu_roster_bad_removed$today <- main_data$today[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#today
# edu_roster_bad_removed$enum_id <- main_data$enum_id[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#enum_name
# edu_roster_bad_removed$resp_gender <- main_data$resp_gender[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#resp_gender
# edu_roster_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#hoh gender
# edu_roster_bad_removed$settlement_idp <- main_data$final_hoh_gender[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# edu_roster_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#FO_In_Charge name
# edu_roster_bad_removed$Name <- main_data$Name[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# edu_roster_bad_removed$deviceid <- main_data$deviceid[match(edu_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
#
# names(edu_roster_bad_removed)[names(edu_roster_bad_removed) == "edu_person_id"] <- "uuid"#####Fix uuid issue

checked_education_data <-  edu_roster_bad_removed %>%

  # run the duplicate unique identifer check
  check_duplicate(
    uuid_column = "uuid"
  )  %>%
  ##chec on others
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(edu_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))


  ) %>%

  # Check for "I don't know" responses in numerical questions
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

##########################################################################################
#############################combine all education cleaning logs##########################
##########################################################################################
edu_cleaning_log <-  checked_education_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )

##########################################################################################################
##################################child_feeding data cleaning is here########################
########################################################################################################
###prepare data for Child feeding data


###############################################################################################################################
#########################################logical list child_feeding(IYCF################################################################
###############################################################################################################################
#############################################################################

child_feeding <- read_excel(df,sheet = "child_feeding")#####import child_feeding roster

child_feeding_bad_removed <- child_feeding %>% #remove surveys that will be deleted
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("_submission__uuid" = "uuid"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::rename("uuid" = child_id) ## ADDED BY ABRAHAM

# child_feeding_bad_removed$admin_1 <- main_data$admin_1[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#region
# child_feeding_bad_removed$admin_2 <- main_data$admin_2[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#district
# child_feeding_bad_removed$today <- main_data$today[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#today
# child_feeding_bad_removed$enum_id <- main_data$enum_id[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#enum_name
# child_feeding_bad_removed$resp_gender <- main_data$resp_gender[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#resp_gender
# child_feeding_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#hoh gender
# child_feeding_bad_removed$settlement_idp <- main_data$settlement_idp[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# child_feeding_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#FO_In_Charge name
# child_feeding_bad_removed$Name <- main_data$Name[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# child_feeding_bad_removed$deviceid <- main_data$deviceid[match(child_feeding_bad_removed$`_submission__uuid`, main_data$uuid)]#deviceid name
#
# names(child_feeding_bad_removed)[names(child_feeding_bad_removed) == "child_id"] <- "uuid"#####Fix uuid issue

child_feeding_bad_removed <- child_feeding_bad_removed %>% filter(caregiver_consent=="yes" | caregiver_consent=="no")

child_feeding_formatted <- format_nut_health_indicators(df = child_feeding_bad_removed,
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


child_feeding_flags <- create_cleaning_log_flags(df = child_feeding_formatted, uuid_col = "hh_id")


###Add the metadata to fo cleaning to match my other logs
child_feeding_flags$admin_1 <- child_feeding_bad_removed$admin_1[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#region
child_feeding_flags$admin_2 <- child_feeding_bad_removed$admin_2[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#district
child_feeding_flags$today <- child_feeding_bad_removed$today[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#today
child_feeding_flags$enum_id <- child_feeding_bad_removed$enum_id[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#enum_name
child_feeding_flags$settlement_idp <- child_feeding_bad_removed$settlement_idp[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#settlement name
child_feeding_flags$FO_In_Charge <- child_feeding_bad_removed$FO_In_Charge[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#FO_In_Charge name
child_feeding_flags$Name <- child_feeding_bad_removed$Name[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#settlement name
child_feeding_flags$deviceid <- child_feeding_bad_removed$deviceid[match(child_feeding_flags$uuid, child_feeding_bad_removed$uuid)]#deviceid name

#######rename the iycf  columns to be easily understandable with the FOs and enumes
child_feeding_flags_renamed <- child_feeding_flags %>% mutate(question.name= case_when(
  question.name=="iycf_1" ~ "breastfed_ever", question.name=="iycf_4" ~ "breastfed_yesterday",
  question.name=="iycf_6a" ~ "water",   question.name=="iycf_6b" ~ "infant_formula_yesterday_yn",
  question.name=="iycf_6b_num" ~ "infant_formula",  question.name=="iycf_6c" ~ "milk_yn",question.name=="iycf_6c_num" ~ "milk",
  question.name=="iycf_6d" ~ "sour_milk_yn", question.name=="iycf_6d_num" ~ "sour_milk", question.name=="iycf_6e" ~ "choco_drink",
  question.name=="iycf_6f" ~ "juice", question.name=="iycf_6g" ~ "soda",  question.name=="iycf_6h" ~ "tea",
  question.name=="iycf_6i" ~ "clear_broth", question.name=="iycf_6j" ~ "water_based",
  question.name=="iycf_7a" ~ "yoghurt_yn",question.name=="iycf_7a_num" ~ "yoghurt",
  question.name=="iycf_7b" ~ "cereals",   question.name=="iycf_7c" ~ "vegetables",  question.name=="iycf_7d" ~ "root_vegetables",
  question.name=="iycf_7e" ~ "leafy_vegetables", question.name=="iycf_7f" ~ "vegetables_other",
  question.name=="iycf_7g" ~ "tropical_fruits", question.name=="iycf_7h" ~ "fruits_other",question.name=="iycf_7i" ~ "organ_meats",
  question.name=="iycf_7k" ~ "meat",question.name=="iycf_7l" ~ "egg", question.name=="iycf_7m" ~ "seafood",
  question.name=="iycf_7j" ~ "canned_meat",  question.name=="iycf_7p" ~ "sweets", question.name=="iycf_7q" ~ "chips",
  question.name=="iycf_7n" ~ "legumes", question.name=="iycf_7o" ~ "cheese",question.name=="iycf_7r" ~ "foods_other",
  question.name=="iycf_8" ~ "child_solid_yesterday", TRUE ~ question.name),
  old.value=case_when(
    question.name== "caregiver_consent" & old.value== 1 ~ "yes",  question.name== "caregiver_consent" & old.value== 2 ~ "no" ,question.name== "caregiver_consent" & old.value== 9 ~ "pnta" ,
    question.name=="breastfed_ever" & old.value== 1 ~ "yes", question.name== "breastfed_ever" & old.value== 2 ~ "no" ,question.name== "breastfed_ever" & old.value== 9 ~ "pnta" ,
    question.name=="breastfed_yesterday" & old.value== 1 ~ "yes",  question.name== "breastfed_yesterday" & old.value== 2 ~ "no" ,question.name== "breastfed_yesterday" & old.value== 9 ~ "pnta" ,
    question.name=="water" & old.value== 1 ~ "yes",  question.name == "water" & old.value== 2 ~ "no" ,question.name== "water" & old.value== 9 ~ "pnta" ,
    question.name=="infant_formula_yesterday_yn" & old.value== 1~ "yes",  question.name== "infant_formula_yesterday_yn" & old.value== 2 ~ "no" ,question.name== "infant_formula_yesterday_yn" & old.value== 9 ~ "pnta" ,
    question.name=="milk_yn" & old.value== 1 ~ "yes",  question.name== "milk_yn" & old.value== 2 ~ "no" ,question.name== "milk_yn" & old.value== 9 ~ "pnta" ,
    question.name=="yoghurt_yn" & old.value== 1 ~ "yes",  question.name== "yoghurt_yn" & old.value== 2 ~ "no" ,question.name== "yoghurt_yn" & old.value== 9 ~ "pnta" ,
    question.name=="sour_milk_yn" & old.value== 1 ~ "yes",  question.name== "sour_milk_yn" & old.value== 2 ~ "no" ,question.name== "sour_milk_yn" & old.value== 9 ~ "pnta" ,
    question.name=="choco_drink" & old.value== 1 ~ "yes",  question.name== "choco_drink" & old.value== 2 ~ "no" ,question.name== "choco_drink" & old.value== 9 ~ "pnta" ,
    question.name=="juice" & old.value== 1~ "yes",  question.name== "juice" & old.value== 2 ~ "no" ,question.name== "juice" & old.value== 9 ~ "pnta" ,
    question.name=="soda" & old.value== 1~ "yes",  question.name== "soda" & old.value== 2 ~ "no" ,question.name== "soda" & old.value== 9 ~ "pnta" ,
    question.name=="tea" & old.value== 1~ "yes",  question.name== "tea" & old.value== 2 ~ "no" ,question.name== "tea" & old.value== 9 ~ "pnta" ,
    question.name=="clear_broth" & old.value== 1~ "yes",  question.name== "clear_broth" & old.value== 2 ~ "no" ,question.name== "clear_broth" & old.value== 9 ~ "pnta" ,
    question.name== "thin_porridge" & old.value== 1~ "yes",  question.name== "thin_porridge" & old.value== 2 ~ "no" ,question.name== "thin_porridge" & old.value== 9 ~ "pnta" ,
    question.name== "water_based" & old.value== 1~ "yes",  question.name== "water_based" & old.value== 2 ~ "no" ,question.name== "water_based" & old.value== 9 ~ "pnta" ,
    question.name=="yesterday_receive_any" & old.value== 1~ "yes",  question.name== "yesterday_receive_any" & old.value== 2 ~ "no" ,question.name== "yesterday_receive_any" & old.value== 9 ~ "pnta" ,
    question.name=="cereals"& old.value== 1~ "yes",  question.name== "cereals" & old.value== 2 ~ "no" ,question.name== "cereals" & old.value== 9 ~ "pnta" ,
    question.name=="vegetables" & old.value== 1~ "yes",  question.name== "vegetables" & old.value== 2 ~ "no" ,question.name== "vegetables" & old.value== 9 ~ "pnta" ,
    question.name=="root_vegetables" & old.value== 1~ "yes",  question.name== "root_vegetables" & old.value== 2 ~ "no" ,question.name== "root_vegetables" & old.value== 9 ~ "pnta" ,
    question.name=="leafy_vegetables" & old.value== 1~ "yes",  question.name== "leafy_vegetables" & old.value== 2 ~ "no" ,question.name== "leafy_vegetables" & old.value== 9 ~ "pnta" ,
    question.name=="vegetables_other"& old.value== 1~ "yes",  question.name== "vegetables_other" & old.value== 2 ~ "no" ,question.name== "vegetables_other" & old.value== 9 ~ "pnta" ,
    question.name=="tropical_fruits" & old.value== 1~ "yes",  question.name== "tropical_fruits" & old.value== 2 ~ "no" ,question.name== "tropical_fruits" & old.value== 9 ~ "pnta" ,
    question.name=="fruits_other" & old.value== 1~ "yes",  question.name== "fruits_other" & old.value== 2 ~ "no" ,question.name== "fruits_other" & old.value== 9 ~ "pnta" ,
    question.name== "organ_meats" & old.value== 1~ "yes",  question.name== "organ_meats" & old.value== 2 ~ "no" ,question.name== "organ_meats" & old.value== 9 ~ "pnta" ,
    question.name== "meat" & old.value== 1~ "yes",  question.name== "meat" & old.value== 2 ~ "no" ,question.name== "meat" & old.value== 9 ~ "pnta" ,
    question.name== "egg" & old.value== 1~ "yes",  question.name== "egg" & old.value== 2 ~ "no" ,question.name== "egg" & old.value== 9 ~ "pnta" ,
    question.name=="seafood" & old.value== 1~ "yes",  question.name== "seafood" & old.value== 2 ~ "no" ,question.name== "seafood" & old.value== 9 ~ "pnta" ,
    question.name=="legumes" & old.value== 1~ "yes",  question.name== "legumes" & old.value== 2 ~ "no" ,question.name== "legumes" & old.value== 9 ~ "pnta" ,
    question.name=="cheese" & old.value== 1~ "yes",  question.name== "cheese" & old.value== 2 ~ "no" ,question.name== "cheese" & old.value== 9 ~ "pnta" ,
    question.name=="foods_other" & old.value== 1~ "yes",  question.name== "foods_other" & old.value== 2 ~ "no" ,question.name== "foods_other" & old.value== 9 ~ "pnta",
    TRUE ~ old.value

  )
)

child_feeding_flags_renamed <-child_feeding_flags_renamed %>% select(-c("issue","feedback")) %>% dplyr::rename(question = question.name,
                                                                                                               old_value = old.value,
                                                                                                               new_value = new.value,
                                                                                                               issue = description,
                                                                                                               change_type= changed) %>%
  mutate(check_binding= paste0(question,uuid))

##########################################################################################################
##################################HH Health data cleaning is here########################
########################################################################################################
###prepare data for Health cleaning

health_roster <- read_excel(df,sheet = "health_ind")#####import health roster

health_roster_bad_removed <- health_roster %>% #remove surveys that will be deleted
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("_submission__uuid" = "uuid"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::rename("uuid" = health_person_id) ## ADDED BY ABRAHAM

# health_roster_bad_removed$admin_1 <- main_data$admin_1[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#region
# health_roster_bad_removed$admin_2 <- main_data$admin_2[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#district
# health_roster_bad_removed$today <- main_data$today[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#today
# health_roster_bad_removed$enum_id <- main_data$enum_id[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#enum_name
# health_roster_bad_removed$resp_gender <- main_data$resp_gender[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#resp_gender
# health_roster_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#hoh gender
# health_roster_bad_removed$settlement_idp <- main_data$final_hoh_gender[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# health_roster_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#FO_In_Charge name
# health_roster_bad_removed$Name <- main_data$Name[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#name of site or settlement name
# health_roster_bad_removed$deviceid <- main_data$deviceid[match(health_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#name deviceid
#
# names(health_roster_bad_removed)[names(health_roster_bad_removed) == "health_person_id"] <- "uuid"#####Fix uuid

checked_health_data <-health_roster_bad_removed %>%

  # run the duplicate unique identifer check
  check_duplicate(
    uuid_column = "uuid",

  ) %>%
  ##chec on others
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(health_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))


  ) %>%
  # Check for "I don't know" responses in numerical questions
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  )

##########################################################################################
#############################combine all health cleaning logs##########################
##########################################################################################
health_cleaning_log <-  checked_health_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )


##########################################################################################################
##################################HH child_vacination data cleaning is here########################
########################################################################################################
###prepare data for child_vacination cleaning

child_vacination_roster <- read_excel(df,sheet = "child_vacination")#####import child_vacination roster

child_vacination_roster_bad_removed <- child_vacination_roster %>% #remove surveys that will be deleted
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("_submission__uuid" = "uuid"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::rename("uuid" = vacine_person_id) ## ADDED BY ABRAHAM

# child_vacination_roster_bad_removed$admin_1 <- main_data$admin_1[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#region
# child_vacination_roster_bad_removed$admin_2 <- main_data$admin_2[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#district
# child_vacination_roster_bad_removed$today <- main_data$today[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#today
# child_vacination_roster_bad_removed$enum_id <- main_data$enum_id[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#enum_name
# child_vacination_roster_bad_removed$resp_gender <- main_data$resp_gender[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#resp_gender
# child_vacination_roster_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#hoh gender
# child_vacination_roster_bad_removed$settlement_idp <- main_data$final_hoh_gender[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# child_vacination_roster_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#FO_In_Charge name
# child_vacination_roster_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#FO_In_Charge name
# child_vacination_roster_bad_removed$Name <- main_data$Name[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#name of settlement
# child_vacination_roster_bad_removed$deviceid <- main_data$deviceid[match(child_vacination_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#name of deviceid
#
# names(child_vacination_roster_bad_removed)[names(child_vacination_roster_bad_removed) == "vacine_person_id"] <- "uuid"#####Fix uuid

checked_child_vacination_data <-child_vacination_roster_bad_removed %>%

  # run the duplicate unique identifer check
  check_duplicate(
    uuid_column = "uuid",

  ) %>%
  ##chec on others
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(child_vacination_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))


  ) %>%
  # Check for "I don't know" responses in numerical questions
  check_value(
    uuid_column = "uuid",
    element_name = "checked_dataset",
    values_to_look = c(99, 999, 9999,-1,-99, -999, -9999,-1)
  )

##########################################################################################
#############################combine all child_vacination cleaning logs##########################
##########################################################################################
child_vacination_cleaning_log <-  checked_child_vacination_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )

##########################################################################################################
##################################HH died_member data cleaning is here########################
########################################################################################################
###prepare data for died mortality cleaning

####logical checks

mortality_check_list <- data.frame(
  name = c("check death","check diff"),
  check = c(
    "final_date_death == dob_died ",
    "final_date_death < dob_died "
  ),
  description = c(
    "logical\\date of death is same as than date of birth,possible but not likely",
    "logical\\date of death is earlier than date of birth, not possible (Died before being born)"
  ),
  columns_to_clean = c("dob_died, final_date_death",
                       "dob_died, final_date_death")
)


died_member_roster <- read_excel(df,sheet = "died_member")#####import died_member roster
died_member_roster <- died_member_roster %>% mutate(uuid=paste0(`_index`,"_",`_submission__uuid`))

died_member_roster_bad_removed <- died_member_roster %>% #remove surveys that will be deleted
  dplyr::filter(`_submission__uuid` %!in% to_delete$uuid) %>%##remove surveys that will be deleted ## ADDED BY ABRAHAM
  dplyr::left_join(main_to_join, by = c("_submission__uuid" = "uuid"),relationship = "many-to-many") %>% ## ADDED BY ABRAHAM
  dplyr::mutate(uuid=paste0(`_index`,"_",`_submission__uuid`))

# died_member_roster_bad_removed$admin_1 <- main_data$admin_1[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#region
# died_member_roster_bad_removed$admin_2 <- main_data$admin_2[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#district
# died_member_roster_bad_removed$today <- main_data$today[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#today
# died_member_roster_bad_removed$enum_id <- main_data$enum_id[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#enum_name
# died_member_roster_bad_removed$resp_gender <- main_data$resp_gender[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#resp_gender
# died_member_roster_bad_removed$hoh_gender <- main_data$final_hoh_gender[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#hoh gender
# died_member_roster_bad_removed$settlement_idp <- main_data$settlement_idp[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#settlement name
# died_member_roster_bad_removed$FO_In_Charge <- main_data$FO_In_Charge[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#FO_In_Charge name
# died_member_roster_bad_removed$Name <- main_data$Name[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#name of settlement
# died_member_roster_bad_removed$deviceid <- main_data$deviceid[match(died_member_roster_bad_removed$`_submission__uuid`, main_data$uuid)]#name of deviceid
#

checked_died_member_data <-died_member_roster_bad_removed %>%

  # run the duplicate unique identifer check
  check_duplicate(
    uuid_column = "uuid",

  ) %>%
  ##chec on others
  check_others(
    uuid_column = "uuid",
    columns_to_check = names(died_member_roster_bad_removed|>
                               dplyr::select(ends_with("_other")) |>
                               dplyr::select(-contains(".")))


  ) %>%
  # Check for "I don't know" responses in numerical questions
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

##########################################################################################
#############################combine all died cleaning logs##########################
##########################################################################################
died_cleaning_log <-  checked_died_member_data %>%
  create_combined_log() %>%
  add_info_to_cleaning_log(
    dataset = "checked_dataset",
    cleaning_log = "cleaning_log",
    information_to_add = c("admin_1","admin_2","today","enum_id","settlement_idp","Name","FO_In_Charge","deviceid")
  )


###########################fcs data is here####################

follow_up <- read_excel("C:/Users/aaron.langat/Documents/R/03_MSNA/01_Data_Cleaning/fsl_cleaning/output/FSL_followup_requests.xlsm", sheet = "Follow-up")
cleaning_logbook <- read_excel("C:/Users/aaron.langat/Documents/R/03_MSNA/01_Data_Cleaning/fsl_cleaning/output/cleaning_logbook.xlsx")

fcs_data <- rbind.fill(follow_up,cleaning_logbook)

fcs_data<- fcs_data %>% dplyr::rename(question= variable, old_value=old.value, old_value=old.value, new_value=new.value,check_id = check.id) %>%
  select(-c("invalid","explanation")) %>% mutate(check_binding= paste0(question,uuid))
###Add the metadata to fo cleaning to match my other logs
fcs_data$admin_1 <- main_data$admin_1[match(fcs_data$uuid, main_data$uuid)]#region
fcs_data$admin_2 <- main_data$admin_2[match(fcs_data$uuid, main_data$uuid)]#district
fcs_data$today <- main_data$today[match(fcs_data$uuid, main_data$uuid)]#today
# fcs_data$enum_id <- main_data$enum_id[match(fcs_data$parent_instance_name, main_data$instance_name)]#enum_name
fcs_data$settlement_idp <- main_data$settlement_idp[match(fcs_data$uuid, main_data$uuid)]#hoh gender
fcs_data$FO_In_Charge <- main_data$FO_In_Charge[match(fcs_data$uuid, main_data$uuid)]#Fo name and base
fcs_data$Name <- main_data$Name[match(fcs_data$uuid, main_data$uuid)]#Name of the settlement
fcs_data$deviceid <- main_data$deviceid[match(fcs_data$uuid, main_data$uuid)]#Name of the deviceid




##############Combine all clogs
Final_clog <- rbind.fill(child_vacination_cleaning_log$cleaning_log,edu_cleaning_log$cleaning_log,health_cleaning_log$cleaning_log,
                         checked_time_cleaning_log$cleaning_log,child_feeding_flags_renamed,died_cleaning_log$cleaning_log,roster_cleaning_log$cleaning_log,
                         wgs_cleaning_log$cleaning_log,fcs_data  )

main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log %>% rbind.fill(Final_clog)#########join clogs to the main data
main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log %>% filter(old_value !="NA")
main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log %>% filter(question !="_index")
main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log %>% filter(old_value !="_id")
main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log %>% filter(old_value !="_parent_index")

########################################################################################################################
########################filtering out clogs that had already been flagged##################################
##########################################################################
main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log %>% mutate(checking_id=paste0(uuid,question))##main_data
previous <- read_excel("03_master/master.xlsx")%>% mutate(checking_id=paste0(uuid,question))##From the previous days
main_cleaning_log$cleaning_log <- filter(main_cleaning_log$cleaning_log, checking_id %!in% previous$checking_id)#removing already checked ones

master <- rbind(main_cleaning_log$cleaning_log,previous) %>% select(-checking_id)#make master file

main_cleaning_log$cleaning_log <- main_cleaning_log$cleaning_log [!duplicated(main_cleaning_log$cleaning_log$checking_id),] %>% select(-checking_id)

###############Export master clog
write.xlsx(master, paste0("03_master/master.xlsx"), overwrite = TRUE)

########################################################splitting cleaning logs by FOs##################################################
##Abdikani
abdikani_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "Abdikani"),
                      cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "Abdikani"))

##Abukar
abukar_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "abukar"),
                    cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "abukar"))

##Daud
daud_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "Daud"),
                  cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "Daud"))

##Isse
isse_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "Isse"),
                  cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "Isse"))

##Kala
kala_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "Kala"),
                  cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "Kala"))

##Omar
omar_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "Omar"),
                  cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "Omar"))

##Suleiman
suleiman_clog <- list(checked_dataset = main_cleaning_log$checked_dataset %>% filter(FO_In_Charge == "Suleiman"),
                      cleaning_log = main_cleaning_log$cleaning_log %>% filter(FO_In_Charge == "Suleiman"))



#############################################Exporting files to respective FO bases######################
##Abdikani
create_xlsx_cleaning_log(abdikani_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/01_Abdikani/Abdikani_cleaning_log_",date_time_now,".xlsx"))


##Abukar
create_xlsx_cleaning_log(abukar_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/02_Abukar/abukar_cleaning_log_",date_time_now,".xlsx"))


##Daud
create_xlsx_cleaning_log(daud_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/03_Daud/daud_cleaning_log_",date_time_now,".xlsx"))

##Isse
create_xlsx_cleaning_log(isse_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/04_Isse/isse_cleaning_log_",date_time_now,".xlsx"))

##Kala
create_xlsx_cleaning_log(kala_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/05_Kala/kala_cleaning_log_",date_time_now,".xlsx"))

##Omar
create_xlsx_cleaning_log(omar_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/06_Omar/omar_cleaning_log_",date_time_now,".xlsx"))

##07_Suleiman
create_xlsx_cleaning_log(suleiman_clog,
                         cleaning_log_name = "cleaning_log",
                         change_type_col = "change_type",
                         column_for_color = "check_binding",
                         header_front_size = 12,
                         header_front_color = "#FFFFFF",
                         header_fill_color = "#ee5859",
                         header_front = "Arial Narrow",
                         body_front = "Arial Narrow",
                         body_front_size = 11,
                         use_dropdown = T,
                         sm_dropdown_type = "numerical",
                         kobo_survey = questions,
                         kobo_choices = choices,
                         output_path = paste0("C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/07_Suleiman/suleiman_cleaning_log_",date_time_now,".xlsx"))





# Rows and columns to blank out

words_to_replace <- c("dnk","pnta")

# Replace the words with blank
main_data$fsl_hhs_nofoodhh <- gsub(paste(words_to_replace, collapse = "|"), "no", main_data$fsl_hhs_nofoodhh)
main_data$fsl_hhs_sleephungry <- gsub(paste(words_to_replace, collapse = "|"), "no", main_data$fsl_hhs_sleephungry)
main_data$fsl_hhs_alldaynight <- gsub(paste(words_to_replace, collapse = "|"), "no", main_data$fsl_hhs_alldaynight)


# # convert integers columns to numeric
cols.integer <- filter(tool.survey, type=="integer")$name
main_data <- mutate_at(main_data, cols.integer, as.numeric)




# cols.integer <- filter(tool.survey, type=="calculate")$name
# df <- mutate_at(df, cols.integer, as.integer)
#
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


# "C:/Users/aaron.langat/ACTED/IMPACT SOM - General/02_Research/01_REACH/Unit 1 - Intersectoral/SOM 24 MSNA/04_Data/03_MSNA_Clogs/002_Cleaning logbook/"
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


#############Clean mortality data
####died_cl
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
