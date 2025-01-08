# Acoustics Data: Import into DB

# Install libraries
library(tidyverse)
library(RPostgreSQL)
library(sf)

# Set up working environment
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

# Read and process moorings data 
# moorings <- read.table("C:\\skh\\MooringSiteMetaData.txt", sep = ",")
# colnames(moorings) <- c("mooring_site_id", "mooring_site_id_full", "latitude", "longitude", "water_depth_m")
# 
# RPostgreSQL::dbWriteTable(con, c("acoustics", "geo_moorings"), moorings, append = TRUE, row.names = FALSE)
# RPostgreSQL::dbSendQuery(con, "UPDATE acoustics.geo_moorings SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")

# Read and process acoustic detections data - based on data received in July 2022 from C. Berchok
# detections <- read.table("C:\\skh\\NewestBeardeds_12Jul2022.csv", sep = ",", header = FALSE, skip = 1)
# colnames(detections) <- c("mooring_site_id", "species", "detection_dt", "num_png_with_call", "num_png_with_effort", "num_sec_with_calls", "num_sec_with_effort",
#                           "s30", "num_png_with_call_30s", "num_png_with_effort_30s",
#                           "s60", "num_png_with_call_60s", "num_png_with_effort_60s", 
#                           "s66", "num_png_with_call_66s", "num_png_with_effort_66s", 
#                           "s90", "num_png_with_call_90s", "num_png_with_effort_90s", 
#                           "s120", "num_png_with_call_120s", "num_png_with_effort_120s", 
#                           "s180", "num_png_with_call_180s", "num_png_with_effort_180s")
# detections <- detections %>%
#   mutate(detection_dt = as.POSIXct(strptime(as.character(detection_dt),format="%d-%b-%Y %H:%M:%S"), tz="GMT")) %>%
#   select("mooring_site_id", "species", "detection_dt", "num_png_with_call", "num_png_with_effort", "num_sec_with_calls", "num_sec_with_effort",
#          "num_png_with_call_30s", "num_png_with_effort_30s",
#          "num_png_with_call_60s", "num_png_with_effort_60s", 
#          "num_png_with_call_90s", "num_png_with_effort_90s", 
#          "num_png_with_call_120s", "num_png_with_effort_120s", 
#          "num_png_with_call_180s", "num_png_with_effort_180s", 
#          "num_png_with_call_66s", "num_png_with_effort_66s")

# Read and process acoustic detections data - based on data received in December 2024 from C. Berchok; processing code from P. Conn
setwd('C:\\Users\\Stacie.Hardy\\Work\\SMK\\Projects\\Acoustics\\Data')

# Files use lat/long
projcrs <- "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"

# Process the moorings metadata file
moorings_df = read.csv("Mooring_Metadata.csv",header=TRUE)
moorings_sf <- st_as_sf(x = moorings_df,                         
                        coords = c("Longitude", "Latitude"),
                        crs = projcrs) %>%
  janitor::clean_names(., "snake") %>%
  # Get rid of blank spaces and 'u' at end
  mutate(mooring_site = substr(mooring_site,1,4)) %>% 
  mutate(mooring_deployment = gsub(" ", "", mooring_deployment))

# Process the bearded seal detection file
detections <- read.csv('60minBins_MaybesAreNo_Beardeds4Paul_10Dec2024.csv', header=TRUE) %>%
  janitor::clean_names(., "snake") %>%
  filter(num_pngs_in_bin > 0) %>% # take out hours without any effort
  mutate(dt = as.POSIXct(bin_start_time, format="%d-%b-%Y %H:%M:%S", tz="GMT")) %>%
  # Create detect_type field and remove species field
  mutate(detect_type = ifelse(species == "InsBearded", "AI", "Human")) %>%
  select(-species)

# Get rid of the "Maybe" columns.  
# Catherine previously included these in the 'no' field so the maybe's are all blank,
Which_column_maybe = grep('maybe', colnames(detections))
detections = detections[,-Which_column_maybe]
detections[is.na(detections)]=0  # replace NAs with zeros

# Import to DB

RPostgreSQL::dbWriteTable(con, c("acoustics", "geo_moorings"), moorings_sf, overwrite = TRUE, row.names = FALSE)
RPostgreSQL::dbWriteTable(con, c("acoustics", "tbl_detections"), detections, overwrite = TRUE, row.names = FALSE)

# Disconnect from DB
RPostgreSQL::dbDisconnect(con)
rm(con)