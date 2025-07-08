### Urban IDP Sampling


## there are districts missing: Burtinle, Eyl, Gebiley, Jariiban, Taleex


# ──────────────────────────────────────────────────────────────────────────────
# 0. Setup
# ──────────────────────────────────────────────────────────────────────────────
library(sf)
library(terra)
library(dplyr)
library(exactextractr)
library(tmap)
library(readxl)
library(igraph)       # for clustering contiguous hexes
library(tidyverse)

# Filepaths
adm2_fp    <- "02_input/03_sampling/admin_2/SOM_ADM2_shapefile.shp"
raster_fp  <- "02_input/03_sampling/pop_density/ghs_pop_e2025_r2023a_54009_100_v1_0_som.tif"
idp_xlsx   <- "02_input/03_sampling/UoA/idp-site-master-list-sep-2024.xlsx"
idp_sheet  <- "CCCM IDP Site List (Verified)"

# CRS & grid
crs_utm    <- 32638
hex_size    <- 1000  # 1 km^2

# ──────────────────────────────────────────────────────────────────────────────
# 1. Read & prep admin + pop raster + IDP points
# ──────────────────────────────────────────────────────────────────────────────


UoA_Admin_Lookup <- read_excel("02_input/03_sampling/UoA_Admin_Lookup.xlsx", sheet = "IDP")


district <- st_read(adm2_fp) %>%
  st_transform(crs_utm) %>%
  left_join(UoA_Admin_Lookup, by = join_by("ADM1_EN" == "adm_1", "ADM2_EN" == "adm_2")) %>%
  filter(!is.na(unit_of_analysis)) %>%
  filter(str_detect(unit_of_analysis, "Gedo|Bay|Galgaduud|Awdal"))


pop_rast   <- rast(raster_fp)

# ──────────────────────────────────────────────────────────────────────────────
# 3. Create 1 km hex grid over regions & punch out IDP buffers
# ──────────────────────────────────────────────────────────────────────────────
# make grid over bay bounding box
hex_grid <- st_make_grid(district, cellsize = hex_size, square = FALSE) %>%
  st_sf() %>%
  st_set_crs(crs_utm) %>%
  st_intersection(district) %>%
  mutate(hex_ID   = paste0("H", sprintf("%06d", row_number())),
         hex_area = as.numeric(st_area(geometry)))  # in m²


hex_grid <- hex_grid[st_geometry_type(hex_grid) %in% c("POLYGON","MULTIPOLYGON"), ]
hex_grid$pop_total <- exact_extract(pop_rast, hex_grid, "sum")
hex_grid <-hex_grid %>%
  mutate(pop_total = ceiling(pop_total))
hex_300 <- filter(hex_grid, pop_total > 300)

# ──────────────────────────────────────────────────────────────────────────────
# 6. Identify towns & suburbs (clusters of hexes ≥300 inh/km² & sum pop ≥5 000)
# ──────────────────────────────────────────────────────────────────────────────
# compute density per km²
hex_nidp <- hex_300 %>%
  mutate(
    hex_km2   = hex_area / 1e6,
    dens_km2  = pop_total / hex_km2
  )

# 1. Keep only high‐density hexes (≥300 inh/km²)
hex_dense <- filter(hex_nidp, dens_km2 >= 300)
n <- nrow(hex_dense)

# 2. Compute neighbors with st_touches()
nbrs_list <- st_touches(hex_dense, hex_dense)

# 3. Build edge list (i < j to avoid duplicates)
edges_df <- do.call(rbind, lapply(seq_along(nbrs_list), function(i) {
  js <- nbrs_list[[i]]
  if (length(js) == 0) return(NULL)
  # keep only j > i
  js <- js[js > i]
  if (length(js) == 0) return(NULL)
  data.frame(from = i, to = js)
}))
# ensure it's a data frame even if empty
edges_df <- edges_df %||% data.frame(from = integer(0), to = integer(0))

# 4. Build igraph, including isolated vertices
vertices_df <- data.frame(name = seq_len(n))
g <- graph_from_data_frame(edges_df, directed = FALSE, vertices = vertices_df)

# 5. Find connected components
comp <- components(g)$membership  # vector of length n, cluster ID per vertex

# 6. Attach cluster IDs back to hex_dense
hex_dense$cluster_id <- comp

# 7. Sum population per cluster, keep only clusters ≥ 5,000
cluster_pops <- hex_dense %>%
  st_drop_geometry() %>%
  group_by(cluster_id) %>%
  summarize(cluster_pop = sum(pop_total), .groups="drop")

good_clusters <- filter(cluster_pops, cluster_pop >= 5000)$cluster_id

# 8. Subset to only hexes in those “good” clusters
hex_urban_centers_idp <- filter(hex_dense, cluster_id %in% good_clusters)


# ──────────────────────────────────────────────────────────────────────────────
# 7.  Now filter IDP sites that fall in urban areas
# ──────────────────────────────────────────────────────────────────────────────

idp_cluster_coords <- read_excel(idp_xlsx, sheet = idp_sheet) %>%
  filter(!is.na(Longitude)) %>%
  left_join(UoA_Admin_Lookup, by = join_by("Region" == "adm_1", "District" == "adm_2")) %>%
  filter(!is.na(unit_of_analysis)) %>%
  filter(str_detect(unit_of_analysis, "Gedo|Bay|Galgaduud|Awdal")) %>%
  select(
    hex_ID = `CCCM IDP Site Code`,
    pop_total = `Individual (Q1-2024)`,
    HH_total = `HH (Q1-2024)`,
    Longitude,
    Latitude
  )  %>%
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326)

hex_urban_centers_idp <- hex_urban_centers_idp %>%
  st_transform(crs = 4326) %>%
  select(geometry, unit_of_analysis)

points_in_hex <- st_join(idp_cluster_coords, hex_urban_centers_idp, join = st_within, left = FALSE)


write_csv(points_in_hex, "03_output/05_sampling/UoA_urban_idp.csv")


tmap_mode("view")
tm_shape(test_hex) +
  tm_polygons("dens_km2", title="Density (inh/km²)", style="quantile") +
  tm_shape(idp_cluster_coords) +
  #tm_text("pop_total") +
  tm_polygons(col="red", alpha=0.3, border="darkred") +
  tm_basemap("OpenStreetMap") +
  tm_layout(title="Bay: Towns & Suburbs (dens≥300 & pop≥5k)")
