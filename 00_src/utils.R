### logical checks list

main_check_list <- data.frame(
  name = c(
    "check migrate",
    "check left",
    "check join",
    "check more_death"
    ),
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

wgs_check_list <- data.frame(
  name = c("Disability more than 2"),
  check = c("number_of_disabilities > 2"),
  description = c("This person has more than2 disabilities, unlikely to happen, please confirm"),
  columns_to_clean = c("wgq_vision,	wgq_hearing,	wgq_mobility,	wgq_cognition,	wgq_self_care,	wgq_communication")
)


hh_check_list <- data.frame(
  name = c("logic_equal",
           "logic_above71"),
  check = c("ind_age != calc_final_age_years",
            "ind_age_months > 60 & ind_age <= 5"),
  description = c("The keyed in age in years, is not the same as the calculated years(Usin the event calendar)",
                  "You indicated that this child is 5 years or less, but you have selected date of birth that is older than 5 years"),
  columns_to_clean = c("ind_age, calc_final_age_years",
                       "final_ind_dob, ind_age")
)






