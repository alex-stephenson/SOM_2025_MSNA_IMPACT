### Urban IDP Sampling

idp_cluster <- read_excel(idp_xlsx, sheet = idp_sheet) %>%
  filter(!is.na(Longitude)) %>%
  filter(Region %in% c("Awdal", "Bay", "Gedo", "Hiiran", "Nugaal", "Sanaag", "Sool", "Togdheer", "Woqooyi Galbeed")) %>%
  select(hex_ID = `CCCM IDP Site Code`, pop_total = `Individual (Q1-2024)`) %>%
  mutate(strata = "Urban IDP")


## rural

