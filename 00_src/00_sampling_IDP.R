### Urban IDP Sampling

UoA_IDP_Admin_Lookup <- read_excel("02_input/03_sampling/UoA_Admin_Lookup.xlsx", sheet = "IDP")

## there are districts missing: Burtinle, Eyl, Gebiley, Jariiban, Taleex

idp_cluster <- read_excel(idp_xlsx, sheet = idp_sheet) %>%
  filter(!is.na(Longitude)) %>%
  left_join(UoA_IDP_Admin_Lookup, by = join_by("Region" == "adm_1", "District" == "adm_2")) %>%
  filter(!is.na(unit_of_analysis)) %>%
  select(hex_ID = `CCCM IDP Site Code`, pop_total = `Individual (Q1-2024)`, HH_total = `HH (Q1-2024)`, unit_of_analysis)


idp_cluster %>%
  write_csv(., "03_output/05_sampling/UoA_urban_idp.csv")






