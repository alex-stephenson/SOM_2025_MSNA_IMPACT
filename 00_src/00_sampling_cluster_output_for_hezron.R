library(readxl)
library(sf)
library(tidyverse)

UoA_urban_sampled <- read_csv("03_output/05_sampling/sample_frame_SOM_MSNA_2025.csv") %>%
  janitor::clean_names() %>%
  filter(str_detect(hex_id, "H")) %>%
  mutate(survey_buffer = survey_buffer * (4/3))

urban_hc_sf <- sf::st_read("03_output/05_sampling/urban_hc/urban_hc_hexagons.shp")

urban_hc_sf_filtered <- urban_hc_sf %>%
  filter(hex_ID %in% UoA_urban_sampled$hex_id) %>%
  left_join(UoA_urban_sampled %>% select(hex_id, survey_buffer), by = join_by("hex_ID" == "hex_id"))

urban_hc_sf_filtered <- urban_hc_sf_filtered %>%
  mutate(location_name = NA) %>%
  select(unit_of_analysis = unt_f_n, hex_id = hex_ID, location_name, district = ADM2_EN, survey_buffer) %>%
  st_drop_geometry()


UoA_IDP_sampled <- read_csv("03_output/05_sampling/sample_frame_SOM_MSNA_2025.csv") %>%
  janitor::clean_names() %>%
  filter(str_detect(hex_id, "CCCM")) %>%
  mutate(survey_buffer = survey_buffer * (4/3))

idp <- "02_input/03_sampling/UoA/idp-site-master-list-sep-2024.xlsx"
idp_sheet  <- "CCCM IDP Site List (Verified)"
all_idp <- read_excel(idp_xlsx, sheet = idp_sheet) %>%
  janitor::clean_names()

UoA_IDP_sampled_filtered <- UoA_IDP_sampled %>%
  left_join(all_idp, by = join_by("hex_id" == "cccm_idp_site_code")) %>%
  select(unit_of_analysis, hex_id, location_name = idp_site, district, survey_buffer) %>%
  st_drop_geometry()

samples_urban_idp <- rbind(urban_hc_sf_filtered, UoA_IDP_sampled_filtered)

samples_urban_idp %>%
  writexl::write_xlsx(., "03_output/05_sampling/sample_frame_plus_metadata.xlsx")


dashboard_sample_hex <- hex_grid %>%
  filter(hex_ID %in% samples_urban_idp$hex_id)
dashboard_sample_idp <- all_idp %>%
  filter(cccm_idp_site_code %in% samples_urban_idp$hex_id)

library(shiny)
library(leaflet)
library(sf)

sf::write_sf(dashboard_sample_hex, "dashboard_sf_output.shp", delete_layer = T)
write_csv(dashboard_sample_idp, "dashboard_csv_output.csv")


# Load your data (replace these with your actual data loading if needed)
# For this example, I assume they are already loaded:
# dashboard_sample_hex
# dashboard_sample_idp

ui <- fluidPage(
  titlePanel("Map with Polygons and Points"),
  leafletOutput("map", height = 600)
)

server <- function(input, output, session) {

  output$map <- renderLeaflet({

    # Create base map
    m <- leaflet() %>%
      addProviderTiles(providers$CartoDB.Positron) %>%  # nice clean basemap

      # Add polygons
      addPolygons(
        data = dashboard_sample_hex,
        color = "#444444",
        weight = 1,
        fillColor = "blue",
        fillOpacity = 0.3,
        popup = ~paste("Polygon ID:", rownames(dashboard_sample_hex))
      ) %>%

      # Add points
      # addCircleMarkers(
      #   data = dashboard_sample_idp,
      #   lng = ~longitude,
      #   lat = ~latitude,
      #   radius = 5,
      #   color = "red",
      #   stroke = TRUE,
      #   fillOpacity = 0.8,
      #   popup = ~paste("Point")
      # )

  })
}

shinyApp(ui, server)

##st_write(urban_hc_sf_filtered, "03_output/05_sampling/urban_hc/urban_hc_sampled_hexagons.shp", delete_layer = T)
