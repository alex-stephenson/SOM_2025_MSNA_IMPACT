### app.R: Shiny dashboard to visualize hex_urban_centers

library(shiny)
library(leaflet)
library(sf)
library(readr)
library(tidyverse)


sampled_hex <- read_csv("03_output/05_sampling/final_sampling_frame.csv")

# -- Load spatial data

rural_hex_path <- "03_output/05_sampling/rural_hc/rural_hc_hexagons.shp"
rural_hex_sf   <- st_read(rural_hex_path)  # contains HEX_ID, pop_total, ADM1_EN, ADM2_EN
rural_hex_sf <- st_transform(rural_hex_sf, crs = 4326)

urban_hex_path <- "03_output/05_sampling/urban_hc/urban_hc_hexagons.shp"
urban_hex_sf   <- st_read(urban_hex_path)  # contains HEX_ID, pop_total, ADM1_EN, ADM2_EN
urban_hex_sf <- st_transform(urban_hex_sf, crs = 4326)

all_uoa <- bind_rows(rural_hex_sf, urban_hex_sf) %>%
  mutate(grouping = if_else(str_detect(unt_f_n, "Urban HC"), "Urban", "Rural"))

all_uoa <- all_uoa %>%
  filter(hex_ID %in% sampled_hex$hex_ID) %>%
  left_join(sampled_hex %>% select(hex_ID, Survey))

ui <- fluidPage(
  titlePanel("SOM MSNA 2025 Proposed Sampling Frame"),
    mainPanel(
      leafletOutput("hexmap", height = "800px", width = "150%")
    )

)

pal <- colorFactor(palette = "RdYlGn", domain = all_uoa$grouping)

server <- function(input, output, session) {
  # Render the leaflet map
  output$hexmap <- renderLeaflet({
    leaflet(all_uoa) %>%
      addTiles() %>%
      addPolygons(
        layerId    = ~hex_ID,
        fillColor = ~pal(grouping),
        color = "black",
        weight     = 1,
        fillOpacity= 0.5,
        highlightOptions = highlightOptions(
          color = "white", weight = 2,
          bringToFront = TRUE
        ),
        label = ~paste0(
          "Hex ID: ", hex_ID, " ~~ ",
          "Pop: ", pop_ttl," ~~ ",
          "Region: ", ADM1_EN," ~~ ",
          "Unit of Analysis:", unt_f_n," ~~ ",
          "Surveys: ", Survey
        ),
        labelOptions = labelOptions(
          direction = "auto",
          textsize  = "12px",
          opacity   = 0.9
        )
      )
  })
}

shinyApp(ui, server)
