##########################################################
# purpose: transform ahah index and domains into quintiles for upload to SeRP
# author: abigail hill
# date: 14/10/2024
##########################################################


#load in version 3 of Access to Healthy Assets and Hazards data
#load packages
library(dplyr)
setwd("//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/AHAH/AHAH_V3")
data <- read.csv("AHAH_V3_0.csv")

lookup<- read.csv("//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/lookups/Lower_Layer_Super_Output_Area_(2011)_to_Built-up_Area_Sub-division_to_Built-up_Area_to_Local_Authority_District_to_Region_(December_2011)_Lookup_in_England_and_Wales.csv")
lookup<- subset(lookup, select=c('LSOA11CD','RGN11NM'))
data<- merge(data, lookup, by.x='lsoa11', by.y='LSOA11CD', all.x=T)
dz<- read.csv("//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/lookups/DataZone2011lookup_2024-07-04.csv")
dz<- subset(dz, select=c('DZ2011_Code','Country_Name'))
colnames(dz)[1]<- c('lsoa11')
colnames(dz)[2]<- c('RGN11NM')
# Merge 'data' and 'dz' based on 'lsoa11'
data_merged <- merge(data, dz, by = 'lsoa11', all.x = TRUE)

# Update the 'RGN11NM' column in 'data' with non-NA values from 'dz'
data_merged$RGN11NM.x[is.na(data_merged$RGN11NM.x)] <- data_merged$RGN11NM.y[is.na(data_merged$RGN11NM.x)]

# Drop the redundant 'RGN11NM.y' column
data_merged <- data_merged[, !names(data_merged) %in% "RGN11NM.y"]

# Optionally, rename 'RGN11NM.x' back to 'RGN11NM'
names(data_merged)[names(data_merged) == 'RGN11NM.x'] <- 'RGN11NM'

data<- data_merged


#Transform the variables to quintiles
data$ah3gp<- ntile(data$ah3gp,5)
# Creating a crosstab and including NA values if present
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3gp, useNA = "ifany"))
# To view the crosstab
print(crosstab)
  data$ah3dent<- ntile(data$ah3dent,5) 
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3dent, useNA = "ifany"))
data$ah3phar<- ntile(data$ah3phar,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3phar, useNA = "ifany"))
data$ah3hosp<- ntile(data$ah3hosp,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3hosp, useNA = "ifany"))
data$ah3blue<- ntile(data$ah3blue,5)   
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3blue, useNA = "ifany"))
data$ah3gpas<- ntile(data$ah3gpas,5)   
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3gpas, useNA = "ifany"))
data$ah3ffood<- ntile(data$ah3ffood,5) 
data$ah3ffood <- 6 - data$ah3ffood
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3ffood, useNA = "ifany"))
data$ah3gamb<- ntile(data$ah3gamb,5)
data$ah3gamb <- 6 - data$ah3gamb
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3gamb, useNA = "ifany"))
data$ah3leis<- ntile(data$ah3leis,5) 
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3leis, useNA = "ifany"))
data$ah3pubs<- ntile(data$ah3pubs,5) 
data$ah3pubs <- 6 - data$ah3pubs
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3pubs, useNA = "ifany"))

data$ah3tob<- ntile(data$ah3tob,5) 
data$ah3tob <- 6 - data$ah3tob
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3tob, useNA = "ifany"))




data$ah3no2<- ntile(data$ah3no2,5)   
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3no2, useNA = "ifany"))
data$ah3so2<- ntile(data$ah3so2,5)  
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3so2, useNA = "ifany"))
data$ah3pm10<- ntile(data$ah3pm10,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3pm10, useNA = "ifany"))

#Subset data to contain variables that are for the TRE
data<- subset(data, select=c('lsoa11','ah3h','ah3g','ah3e','ah3r','ah3ahah_rn',
                             'ah3gp',"ah3dent",      "ah3phar",      "ah3hosp",     
                             "ah3blue",      "ah3gpas",      "ah3ffood",     "ah3gamb",      "ah3leis",     
                             "ah3pubs",      "ah3tob",       "ah3no2",       "ah3so2",       "ah3pm10","RGN11NM"))




data$ah3h<- ntile(data$ah3h,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3h, useNA = "ifany"))


data$ah3g<- ntile(data$ah3g,5)
crosstab <- as.data.frame(table(data$ah3g, data$ah3h, useNA = "ifany"))

data$ah3e<- ntile(data$ah3e,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3e, useNA = "ifany"))
data$ah3r<- ntile(data$ah3r,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3r, useNA = "ifany"))
data$ah3ahah_rn<- ntile(data$ah3ahah_rn,5)
crosstab <- as.data.frame(table(data$RGN11NM, data$ah3ahah_rn, useNA = "ifany"))
#save data
colnames(data)[1]<-c('geo_unit')
colnames(data)[2]<-c('health_domain')
colnames(data)[3]<-c('greenspace_domain')
colnames(data)[4]<-c('air_domain')
colnames(data)[5]<-c('retail_domain')
colnames(data)[6]<-c('ahah_index')

write.csv(data, 'ahah_version3_transformed.csv')
