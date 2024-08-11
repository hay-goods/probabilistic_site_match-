#####libraries#####
library(tidyr)
library(readxl)
library(dplyr)
library(RecordLinkage)
library(geosphere)
library(sqldf)
library(stringdist)
library(reshape2)
library(postmastr)
library(magrittr)
library(tidycensus)
library(stringr)
library(writexl)

#####import#####
data1 <-  read_excel("./.xlsx")
data2 <-  read_excel("./.xlsx")

####functions####
# Remove special characters
remove_special_characters <- function(x) {
  gsub("[^A-Za-z0-9\\s]", "", x)
}
# Remove medical terms words from matching name
remove_words <- function(data, column_name) {
  data[[paste0(column_name, "_clean")]] <- str_replace_all(data[[column_name]], 
            regex("\\b(?:MEDICAL|HEALTH|HOSPITAL|GROUP|CENTER|FAMILY|CARE|
                    PRIMARY|CLINICS|CLINIC|PEDIATRICS|PEDIATRIC|SERVICES|
                    SERVICE|MEDICINE|TREATMENT|PROGRAMS|AFTERCARE|NURSING|
                    COMMUNITY|PUBLIC|DEPARTMENT|DENTAL|REGIONAL|
                    MEMORIAL|ASSOCIATION|CORPORATION|COUNTY|WELLNESS)\\b",
                                                    ignore_case = TRUE), ""
                  )
  return(data)
}

####Transformations####
#### data1 ####
data1x <- data1

#zip cleaning
data1x$zipshortp <-  substr(as.character(data1x$postalCode), 1, 5) 

##Upcase and Trim String variables
data1x$siteName <- toupper(trimws(data1x$siteName))
data1x$siteddress <- toupper(trimws(data1x$siteAddress))
data1x$siteAddress2 <- toupper(trimws(data1x$siteAddress2))
data1x$city <- toupper(trimws(data1x$city))
data1x$state <- toupper(trimws(data1x$state))
data1x$`site Email` <- toupper(trimws(data1x$`site Email`))

#site name cleaning
data1x <- remove_words(data1x,"siteName")
data1x$SiteName_clean <- ifelse(data1x$SiteName_clean == "", NA, data1x$SiteName_clean)
data1x$SiteName_clean <- remove_special_characters(data1x$SiteName_clean)

#Phone cleaning/parsing
data1x$cleaned_phone <- remove_special_characters(data1x$phone)
data1x$phone1 <- substr(data1x$cleaned_phone, 1, 3)
data1x$phone2 <- substr(data1x$cleaned_phone, 4, 6)
data1x$phone3 <- substr(data1x$cleaned_phone, 7, nchar(data1x$cleaned_phone))

##Address cleaning/parsing
data1_add <- data1x %>%
  pm_identify(var = "siteAddress") 

data1_add2 <- data1_add %>%
  pm_prep(var = "siteAddress", type = "street") 

#house number
data1_add2 <- data1_add2 %>% pm_house_parse()

#street directions
dirsDict <- pm_dictionary(type = "directional", locale = "us")
data1_add3 <- data1_add2 %>% 
  pm_streetDir_parse(dictionary = dirsDict) %>%
  pm_streetSuf_parse() %>%
  pm_street_parse(ordinal = TRUE, drop = TRUE)

pm_replace(data1_add3, source = data1_add2)

#join address data
cols_to_select <- c("pm.house", "pm.preDir", "pm.street","pm.streetSuf","pm.sufDir")
data1_add <- data1_add %>% rename (pm_uid = pm.uid) 
data1_add3 <- data1_add3 %>% rename (pm_uid = pm.uid)
# Perform left join and select specific columns
queryp <- "
    SELECT t1.*,
      t2.*
    FROM data1_add t1
    LEFT JOIN data1_add3 t2
    ON  (t1.pm_uid = t2.pm_uid )
"
data1_addx <- sqldf(queryp)
data1_addx <- data1_addx %>%
  select(-c( "pm.id","pm_uid","pm.type"
  )) 

##Upcase and Trim String variables
data1_addx$pm.house <- toupper(trimws(data1_addx$pm.house))
data1_addx <- data1_addx %>% rename (pm_house_data1 = pm.house) 

data1_addx$pm.preDir <- toupper(trimws(data1_addx$pm.preDir))
data1_addx <- data1_addx %>% rename (pm_preDir_data1 = pm.preDir) 

data1_addx$pm.street <- toupper(trimws(data1_addx$pm.street))
data1_addx <- data1_addx %>% rename (pm_street_data1 = pm.street) 

data1_addx$pm.streetSuf <- toupper(trimws(data1_addx$pm.streetSuf))
data1_addx <- data1_addx %>% rename (pm_streetSuf_data1 = pm.streetSuf) 

data1_addx$pm.sufDir <- toupper(trimws(data1_addx$pm.sufDir))
data1_addx <- data1_addx %>% rename (pm_sufDir_data1 = pm.sufDir) 

data1_addx$state <- toupper(trimws(data1_addx$state))

#round long/lat to 4 decimals
data1_addx$latitude <- round(data1_addx$latitude, 4)
data1_addx$longitude <- round(data1_addx$longitude, 4)

####data2####
data2x <- data2

##Upcase and Trim String variables
data2x$`FACILITY NAME` <- toupper(trimws(data2x$`FACILITY NAME`))
data2x$`FACILITY TYPE` <- toupper(trimws(data2x$`FACILITY TYPE`))
data2x$STREET <- toupper(trimws(data2x$STREET))
data2x$CITY <- toupper(trimws(data2x$CITY))
data2x$WEBSITE <- toupper(trimws(data2x$WEBSITE))

#clean site name
data2x <- remove_words(data2x,"FACILITY NAME")
data2x$`FACILITY NAME_clean` <- ifelse(data2x$`FACILITY NAME_clean` == "", NA, data2x$`FACILITY NAME_clean`)
data2x$`FACILITY NAME_clean` <- remove_special_characters(data2x$`FACILITY NAME_clean`)

#round long/lat to 4 decimals
data2x$LATITUDE <- round(data2x$LATITUDE, 4)
data2x$LONGITUDE <- round(data2x$LONGITUDE, 4)

#Phone cleaning/parsing
data2x$cleaned_phone1 <- remove_special_characters(data2x$PHONE1)
data2x$phone1_area <- substr(data2x$cleaned_phone1, 1, 3)
data2x$phone1_2nd <- substr(data2x$cleaned_phone1, 4, 6)
data2x$phone1_3rd <- substr(data2x$cleaned_phone1, 7, nchar(data2x$cleaned_phone1))

data2x$cleaned_phone2 <- remove_special_characters(data2x$PHONE2)
data2x$phone2_area <- substr(data2x$cleaned_phone2, 1, 3)
data2x$phone2_2nd <- substr(data2x$cleaned_phone2, 4, 6)
data2x$phone2_3rd <- substr(data2x$cleaned_phone2, 7, nchar(data2x$cleaned_phone2))

data2x$cleaned_phone3 <- remove_special_characters(data2x$PHONE3)
data2x$phone3_area <- substr(data2x$cleaned_phone3, 1, 3)
data2x$phone3_2nd <- substr(data2x$cleaned_phone3, 4, 6)
data2x$phone3_3rd <- substr(data2x$cleaned_phone3, 7, nchar(data2x$cleaned_phone2))

##Address cleaning/parsing
data2_add <- data2x %>%
  pm_identify(var = "STREET") 

data2_add2 <- data2_add %>%
  pm_prep(var = "STREET", type = "street") 

#house number
data2_add2 <- data2_add2 %>% pm_house_parse()

#street directions
dirsDict <- pm_dictionary(type = "directional", locale = "us")
data2_add3 <- data2_add2 %>% 
  pm_streetDir_parse(dictionary = dirsDict) %>%
  pm_streetSuf_parse() %>%
  pm_street_parse(ordinal = TRUE, drop = TRUE)

pm_replace(data2_add3, source = data2_add2)

#join address data
cols_to_select <- c("pm.house", "pm.preDir", "pm.street","pm.streetSuf","pm.sufDir")
data2_add <- data2_add %>% rename (pm_uid = pm.uid) 
data2_add3 <- data2_add3 %>% rename (pm_uid = pm.uid)
# Perform left join and select specific columns
queryi <- "
    SELECT t1.*,
      t2.*
    FROM data2_add t1
    LEFT JOIN data2_add3 t2
    ON  (t1.pm_uid = t2.pm_uid )
"
data2_addx <- sqldf(queryi)
data2_addx <- data2_addx %>%
  select(-c( "pm.id","pm_uid","pm.type"
  )) 

##Upcase and Trim String variables
data2_addx$pm.house <- toupper(trimws(data2_addx$pm.house))
data2_addx <- data2_addx %>% rename (pm_house_data2 = pm.house) 

data2_addx$pm.preDir <- toupper(trimws(data2_addx$pm.preDir))
data2_addx <- data2_addx %>% rename (pm_preDir_data2 = pm.preDir) 

data2_addx$pm.street <- toupper(trimws(data2_addx$pm.street))
data2_addx <- data2_addx %>% rename (pm_street_data2 = pm.street) 

data2_addx$pm.streetSuf <- toupper(trimws(data2_addx$pm.streetSuf))
data2_addx <- data2_addx %>% rename (pm_streetSuf_data2 = pm.streetSuf) 

data2_addx$pm.sufDir <- toupper(trimws(data2_addx$pm.sufDir))
data2_addx <- data2_addx %>% rename (pm_sufDir_data2 = pm.sufDir) 

data2_addx$STATE <- toupper(trimws(data2_addx$STATE))

####MATCH BLOCKING####
## join data1 to data2
# Block 1A - first 6 of phone1 and zip match
query1A <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.phone1  = t2.phone1_area
    AND t1.phone2  = t2.phone1_2nd
    AND t1.zipshortp  = t2.ZIP
"
block1A <- sqldf(query1A)
block1A$match_type <-  "1.phone1_zip"

# Block 1B - first 6 of phone2 and zip match
query1B <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.phone1  = t2.phone2_area
    AND t1.phone2  = t2.phone2_2nd
    AND t1.zipshortp  = t2.ZIP
"
block1B <- sqldf(query1B)
block1B$match_type <-  "1.phone2_zip"

# Block 1c - first 6 of phone3 and zip
# query1C <- "
#     SELECT *
#     FROM data1_addx t1
#     INNER JOIN data2_addx t2
#     ON  t1.phone1  = t2.phone3_area
#     AND t1.phone2  = t2.phone3_2nd
#     AND t1.zipshortp  = t2.ZIP
# "
# block1C <- sqldf(query1C)
# block1C$match_type <-  "phone3_zip"

# Block 1D - full phone matches
query1D <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.phone1  = t2.phone1_area
    AND t1.phone2  = t2.phone1_2nd
    AND t1.phone3  = t2.phone1_3rd
"
block1D <- sqldf(query1D)
block1D$match_type <-  "1.full_phone1"

# Block 1D - full phone matches
query1E <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.phone1  = t2.phone2_area
    AND t1.phone2  = t2.phone2_2nd
    AND t1.phone3  = t2.phone2_3rd
"
# Block 1D - full phone matches
block1E <- sqldf(query1E)
block1E$match_type <-  "1.full_phone2"
# query1F <- "
#     SELECT *
#     FROM data1_addx t1
#     INNER JOIN data2_addx t2
#     ON  t1.phone1  = t2.phone3_area
#     AND t1.phone2  = t2.phone3_2nd
#     AND t1.phone3  = t2.phone3_3rd
# "
# block1F <- sqldf(query1F)
# block1F$match_type <-  "full_phone3"

#Block 2#
# Block 2A - first 6 of phone1 and street address match
query2A <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.phone1  = t2.phone1_area
    AND t1.phone2  = t2.phone1_2nd
"
block2A <- sqldf(query2A)
block2A$match_type <-  "2.phone1"
query2B <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.phone1  = t2.phone2_area
    AND t1.phone2  = t2.phone2_2nd
"

# Block 2B - first 6 of phone2 and street address match
block2B <- sqldf(query2B)
block2B$match_type <-  "2.phone2"

# Block 2C - first 6 of phone3 and street address match
# query2C <- "
#     SELECT *
#     FROM data1_addx t1
#     INNER JOIN data2_addx t2
#     ON  t1.phone1  = t2.phone3_area
#     AND t1.phone2  = t2.phone3_2nd
# "
# block2C <- sqldf(query2C)
# block2C$match_type <-  "phone3"

# Block 2D - zip and street address match
query2D <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.pm_street_data1 = t2.pm_street_data2
    AND t1.zipshortp  = t2.ZIP

"
block2D <- sqldf(query2D)
block2D$match_type <-  "2.zip_street"

#Block 3  #
# Block 3 - first 3 of zip and street address match
query3 <- "
    SELECT *
    FROM data1_addx t1
    INNER JOIN data2_addx t2
    ON  t1.zipshortp  = t2.ZIP
"
block3 <- sqldf(query3)
block3$match_type <-  "3.zip"

#Block 4 - Site Name Scores & State#
# Calculate Jaccard similarity and Levenshtein distance for each pair
query4 <- "
    SELECT *
    FROM data1_addx t1
    LEFT JOIN data2_addx t2
    ON  t1.state  = t2.STATE
"
block4 <- sqldf(query4)
block4$match_type <-  "4.state_name"

#Append blocks - blocks 1C. 1F, 2C had no results to merge
combined_block <- rbind(block1A, block1B, block1D, block1E, block2A, block2B, block2D, block3)

# Calculate Jaccard similarity and Levenshtein distance for all site names
##Levenshtein 
combined_block$name_lev <- stringdist::stringdist(combined_block$`FACILITY NAME_clean`, combined_block$providerSiteName_clean, method = "lv")
block4$name_lev <- stringdist::stringdist(block4$`FACILITY NAME_clean`, block4$providerSiteName_clean, method = "lv")
##Jaccard similarity
combined_block$name_jac <- stringdist::stringdist(combined_block$`FACILITY NAME_clean`, combined_block$providerSiteName_clean, method = "jaccard")
block4$name_jac <- stringdist::stringdist(block4$`FACILITY NAME_clean`, block4$providerSiteName_clean, method = "jaccard")


####calculations####
combined_block$distance <- round(distGeo(combined_block[, c("LONGITUDE", "LATITUDE")], 
                                         combined_block[, c("longitude", "latitude")]) * 0.000621371, 2)
block4$distance <- round(distGeo(block4[, c("LONGITUDE", "LATITUDE")], 
                                 block4[, c("longitude", "latitude")]) * 0.000621371, 2)

##FILTERS##
combined_blockx <- combined_block %>%
  select (c(
  # select variables of interest
    )
  )
block4x <- block4 %>%
  select (c(
    # match variables above
    )
  )%>%
  ## filter by desired spelling distance figure
  filter( (name_jac == 0 | name_jac > 0.50) & 
            (name_lev == 0 | name_lev > 50) )

combined_blockxx <- rbind(combined_blockx, block4x)
combined_blockxxx <- combined_blockxx[!duplicated(combined_blockxx[, c("PIN_data1", "PIN_data2")]), ] 

#split one-to-one matches and many-to-many matches
combined_blockxxx <- combined_blockxxx %>% 
  mutate(index_label = dense_rank(provPIN_data1))

single_index_df <- combined_blockxxx %>%
  group_by(index_label) %>%
  filter(n() == 1) %>%
  ungroup()

# Subset rows with duplicate index values into another dataframe
duplicate_index_df <- combined_blockxxx %>%
  group_by(index_label) %>%
  filter(n() > 1) %>%
  ungroup()

# Anti-Join all data that didn't match
data1_addx$PIN_data1 <- data1_addx$PIN
anti_join_df <- data1_addx %>%
  anti_join(single_index_df, by = "PIN_data1") %>%
  anti_join(duplicate_index_df, by = "PIN_data1")

####EXPORT####
file_path <- "./cmb_data.xlsx"
# write a multitab xlsx export
write_xlsx(list(single_index_df = single_index_df, duplicate_index_df = duplicate_index_df, anti_join_df = anti_join_df), file_path)
