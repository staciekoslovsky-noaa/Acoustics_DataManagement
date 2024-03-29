# Acoustics Data: Import into DB

# Install libraries
library(tidyverse)
library(RPostgreSQL)

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

# Read and process acoustic detections data 
detections <- read.table("C:\\skh\\NewestBeardeds_12Jul2022.csv", sep = ",", header = FALSE, skip = 1)
colnames(detections) <- c("mooring_site_id", "species", "detection_dt", "num_png_with_call", "num_png_with_effort", "num_sec_with_calls", "num_sec_with_effort",
                          "s30", "num_png_with_call_30s", "num_png_with_effort_30s",
                          "s60", "num_png_with_call_60s", "num_png_with_effort_60s", 
                          "s66", "num_png_with_call_66s", "num_png_with_effort_66s", 
                          "s90", "num_png_with_call_90s", "num_png_with_effort_90s", 
                          "s120", "num_png_with_call_120s", "num_png_with_effort_120s", 
                          "s180", "num_png_with_call_180s", "num_png_with_effort_180s")
detections <- detections %>%
  mutate(detection_dt = as.POSIXct(strptime(as.character(detection_dt),format="%d-%b-%Y %H:%M:%S"), tz="GMT")) %>%
  select("mooring_site_id", "species", "detection_dt", "num_png_with_call", "num_png_with_effort", "num_sec_with_calls", "num_sec_with_effort",
         "num_png_with_call_30s", "num_png_with_effort_30s",
         "num_png_with_call_60s", "num_png_with_effort_60s", 
         "num_png_with_call_90s", "num_png_with_effort_90s", 
         "num_png_with_call_120s", "num_png_with_effort_120s", 
         "num_png_with_call_180s", "num_png_with_effort_180s", 
         "num_png_with_call_66s", "num_png_with_effort_66s")

RPostgreSQL::dbWriteTable(con, c("acoustics", "tbl_detections"), detections, overwrite = TRUE, row.names = FALSE)

# Disconnect from DB
RPostgreSQL::dbDisconnect(con)
rm(con)
