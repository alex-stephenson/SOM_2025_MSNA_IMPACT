rm(list = ls())


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


UoA_Admin_Lookup <- read_excel("02_input/03_sampling/UoA_Admin_Lookup.xlsx", sheet = "HC")


district <- st_read(adm2_fp) %>%
  st_transform(crs_utm) %>%
  left_join(UoA_Admin_Lookup, by = join_by("ADM1_EN" == "adm_1", "ADM2_EN" == "adm_2")) %>%
  filter(!is.na(unit_of_analysis))

pop_rast   <- rast(raster_fp)

idp_sites <- read_excel(idp_xlsx, sheet = idp_sheet) %>%
  filter(!is.na(Longitude)) %>%
  st_as_sf(coords = c("Longitude","Latitude"), crs = 4326) %>%
  st_transform(crs_utm) %>%
  rename(idp_pop = `Individual (Q1-2024)`) %>%
  mutate(.row = row_number()) %>%
  filter(Region %in% UoA_Admin_Lookup$adm_1)

# ──────────────────────────────────────────────────────────────────────────────
# 2. Compute point‐specific buffers (raster-derived density)
# ──────────────────────────────────────────────────────────────────────────────

# 2a) Add an explicit row ID to the sf
idp_sites <- idp_sites %>%
  mutate(.row = row_number())

# 2b) Convert to SpatVector and extract pop/100m cell value
idp_vec <- terra::vect(idp_sites)

dens_df <- terra::extract(
  pop_rast,
  idp_vec,
  fun = mean,    # mean over the cell(s) your point falls into
  na.rm = TRUE,
  ID = TRUE      # include input row ID in the output
)

# dens_df now has columns: ID (input row), and the raster value
names(dens_df)[2] <- "dens_per_100m2"

# 2c) Join back to sf by .row
idp_sites <- idp_sites %>%
  left_join(dens_df, by = c(".row" = "ID")) %>%
  select(-.row)

# 2d) Convert density to per‐hectare (×100) and fill NAs with median
idp_sites <- idp_sites %>%
  mutate(
    dens_ha = dens_per_100m2 * 100,                # from per-100m² → per-ha
    dens_ha = ifelse(is.na(dens_ha) | dens_ha == 0, median(dens_ha, na.rm=TRUE), dens_ha)
  )

# 2e) Compute area (ha) & radius (m)
idp_sites <- idp_sites %>%
  mutate(
    area_ha  = idp_pop / dens_ha,
    radius_m = sqrt((area_ha * 10000) / pi),
    radius_m = ifelse(radius_m == 0, median(radius_m, na.rm = T), radius_m),
    radius_m = ifelse(radius_m < 20, 20, radius_m)
  )

# 2f) Buffer with smoother circles, union, simplify
idp_buffers <- idp_sites %>%
  st_buffer(dist = .$radius_m, nQuadSegs = 100) %>%
  st_union() %>%
  st_simplify(dTolerance = 5)




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

# subtract IDP areas
hex_grid_cut <- st_difference(hex_grid, idp_buffers)

# ──────────────────────────────────────────────────────────────────────────────
# 4. Extract populations & filter by >300 total pop
# ──────────────────────────────────────────────────────────────────────────────

hex_grid_cut <- hex_grid_cut[st_geometry_type(hex_grid_cut) %in% c("POLYGON","MULTIPOLYGON"), ]
hex_grid_cut$pop_total <- exact_extract(pop_rast, hex_grid_cut, "sum")
hex_grid_cut <-hex_grid_cut %>%
  mutate(pop_total = ceiling(pop_total))
hex_300 <- filter(hex_grid_cut, pop_total > 300)

# ──────────────────────────────────────────────────────────────────────────────
# 5. Compute IDP pop per hex & remove hexes >30% IDP
# ──────────────────────────────────────────────────────────────────────────────

# 1. Build individual buffers (preserving idp_pop) as an sf
idp_buffers_indiv <- idp_sites %>%
  st_buffer(dist = .$radius_m, nQuadSegs = 100) %>%
  st_as_sf()   # now it's an sf with idp_pop and radius_m columns

# 2. Intersect those buffers with your selected hexes
#    This gives you one row per (buffer × hex) overlap
idp_in_hex <- st_intersection(
  idp_buffers_indiv %>% select(idp_pop),
  hex_300 %>% select(hex_ID)
)

# 3. Sum the buffer populations per hex_ID
idp_by_hex <- idp_in_hex %>%
  st_drop_geometry() %>%
  group_by(hex_ID) %>%
  summarize(idp_pop = sum(idp_pop, na.rm = TRUE), .groups="drop")

# 4. Join back and compute proportion
hex_final <- hex_300 %>%
  left_join(idp_by_hex, by = "hex_ID") %>%
  mutate(
    idp_pop  = tidyr::replace_na(idp_pop, 0),
    prop_idp = idp_pop / pop_total
  ) %>%
  filter(prop_idp <= 0.50)

# ──────────────────────────────────────────────────────────────────────────────
# 6. Identify towns & suburbs (clusters of hexes ≥300 inh/km² & sum pop ≥5 000)
# ──────────────────────────────────────────────────────────────────────────────
# compute density per km²
hex_nidp <- hex_final %>%
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
hex_urban_centers <- filter(hex_dense, cluster_id %in% good_clusters) %>%
  mutate(HH_total = ceiling(pop_total / 6))

st_write(hex_urban_centers, "03_output/05_sampling/urban_hc/urban_hc_hexagons.shp", delete_layer = T)

# ──────────────────────────────────────────────────────────────────────────────
# 7. Export CSV
# ──────────────────────────────────────────────────────────────────────────────
out_df <- hex_urban_centers %>%
  st_drop_geometry() %>%
  select(hex_ID, pop_total, HH_total, idp_pop, prop_idp, dens_km2, cluster_id, ADM2_EN, unit_of_analysis)

write.csv(out_df, "03_output/05_sampling/UoA_Urban.csv", row.names = FALSE)

# ──────────────────────────────────────────────────────────────────────────────
# 8. Interactive map
# ──────────────────────────────────────────────────────────────────────────────
tmap_mode("view")
tm_shape(hex_urban_centers) +
  tm_polygons("dens_km2", title="Density (inh/km²)", style="quantile") +
  #tm_text("pop_total") +
  tm_shape(idp_buffers, size= 0.5) +
  tm_polygons(col="red", alpha=0.3, border="darkred") +
  tm_basemap("OpenStreetMap") +
  tm_layout(title="Bay: Towns & Suburbs (dens≥300 & pop≥5k)")
