
library(sf)
library(terra)
library(dplyr)
library(sampling)
library(exactextractr)

districts <- st_read("02_input/03_sampling/admin_2/SOM_ADM2_shapefile.shp")
districts_proj <- st_transform(districts, 32638) %>%
  st_make_grid(.)

units     <- st_read("02_input/03_sampling/livelihood_zones/SOM_Shapefiles_LZ.shp") %>%
  st_transform(., 32638)
pop_rast  <- rast("02_input/03_sampling/pop_density/ghs_pop_e2025_r2023a_54009_100_v1_0_som.tif")
##camps     <- st_read("camps.shp")    # optional

hex_full <- st_make_grid(districts_proj, cellsize = 1000, square = FALSE) %>%
  st_sf() %>%
  st_intersection(districts_proj)

##hex_clean <- st_difference(hex_full, st_union(camps))  # remove camps

frags <- st_intersection(hex_full, units) %>%
  mutate(pop_raw = exact_extract(pop_rast, ., "sum"))

threshold <- 25
frags <- frags %>% filter(pop_raw > threshold)


### step here to do something with camp numbers

hex_pop <- frags %>%
  group_by(hex_ID) %>%
  summarize(pop = sum(pop_corr)) %>%
  ungroup() %>%
  mutate(cluster_ID = paste0(substr(district,1,3), row_number()))


hex_pop <- hex_pop %>%
  mutate(HH_pop = round(pop / persons_per_hh)) ### here i need to add in the HH size calculation

#### cluster sampling

# Define sampling frame
frame <- hex_pop %>%
  select(district, cluster_ID, HH_pop)

# Use sampling::strata() or sampling::cluster() to select clusters
selected <- sampling::strata(frame,
                             stratanames = "district",
                             size = clusters_per_stratum,
                             method = "srswor")


sel_polys <- hex_clean %>% filter(cluster_ID %in%
                                    selected$cluster_ID)
points <- st_sample(
  sel_polys,
  size = cluster_points,
  type = "random")


st_write(selected, "sample_frame.csv")
st_write(sel_polys, "selected_clusters.shp")
st_write(points, "survey_points.shp")

