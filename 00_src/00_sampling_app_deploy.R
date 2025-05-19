library(tidyverse)


UoA_urban <- read_csv("03_output/05_sampling/UoA_Urban.csv")
UoA_urban_idp <- read_csv("03_output/05_sampling/UoA_urban_idp.csv")
hc_uoa <- bind_rows(UoA_urban, UoA_urban_idp)

hc_uoa <- hc_uoa %>%
  filter(str_detect(unit_of_analysis, "Gedo|Bay|Galgaduud|Awdal"))


hc_uoa %>% write_csv(., "03_output/05_sampling/all_clusters.csv")


deploy_app_input <- list.files(full.names = T, recursive = T) %>%
  keep(~ str_detect(.x, "05_site_data")
       | str_detect(.x, "app.R")
       | str_detect(.x, "hexagons")
       | str_detect(.x, "hc_uoa_sample.csv")
       | str_detect(.x, "final_sampling_frame"
       ))

#rsconnect::deployApp(appFiles =deploy_app_input, appName = "REACH_SOM_SCC_Baseline_Field_Dashboard", account = "impact-initiatives")



rsconnect::deployApp(appFiles =deploy_app_input,
                     appDir = ".",
                     appPrimaryDoc = "./00_src/spatial_mapping_app.R",
                     appName = "REACH_MSNA_Sampling_Dashboard",
                     account = "impact-initiatives")




hc_uoa %>%
  group_by(unit_of_analysis) %>%
  summarise(n = n(),
            total_pop = sum(pop_total),
            total_HH = sum(HH_total)) %>%
  View()
