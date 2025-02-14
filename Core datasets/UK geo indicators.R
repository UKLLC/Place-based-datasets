######################################
#Purpose: Create core geo indicator files for all 4 UK nations to upload to SeRP
#Author: Abigail Hill
#Date: 14/06/24
######################################

#Load in libraries
library(sf)
library(dplyr)

#set working directory

#read in English IMD (ESRI Shapefile)
#There are 32,844 LSOA's in England 
english<- st_read("//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/English IMD 2019/English IMD 2019/")

#transform to data frame
english$geometry<- NULL
#Rename lsoa column 
colnames(english)[1]<- c('Granularity')

#read in Welsh IMD
#There are 1,909 lsoa's in wales
welsh<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/Welsh IMD 2019/Welsh IMD 2019/WIMD2019_Ranks.csv')
#Rename lsoa column
colnames(welsh)[1]<- c('Granularity')


#read in Scottish IMD
#There are 6,976 Data Zones in Scotland
scottish<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/Scottish IMD 2020/simd2020_withinds.csv')
#Rename data zone column
colnames(scottish)[1]<- c('Granularity')

#read in Northern Irish IMD
#There are 890 SOA's in Northern Ireland
irish<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/Northern Irish MDM 2017/Northern Ireland MDM.csv')
#Rename SOA column
colnames(irish)[1]<- c('Granularity')

#read in combined IMD
#There are lsoa 42,619 equivalents in the UK
combined<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/uk_imd2019_cdrc.csv')
colnames(combined)[1]<- c('Granularity')



#English IMD: create quintiles for each domain and overall IMD score
#Invert ranking
english$imd2019eng<- ntile(-english$IMDScore,5)
#Invert ranking
english$imd2019eng_income<-ntile(-english$IncScore,5)
#Invert ranking
english$imd2019eng_employment<-ntile(-english$EmpScore,5)
#Invert ranking
english$imd2019eng_education<-ntile(-english$EduScore,5)
#Invert ranking
english$imd2019eng_health<-ntile(-english$HDDScore,5)
#Invert ranking
english$imd2019eng_crime<-ntile(-english$CriScore,5)
#Invert ranking
english$imd2019eng_barriers<-ntile(-english$BHSScore,5)
#Invert ranking
english$imd2019eng_environment<-ntile(-english$EnvScore,5)


#Drop columns not needed in English IMD
english<- subset(english, select=c('Granularity',
                             'imd2019eng',
                             'imd2019eng_income',
                             'imd2019eng_employment',
                             'imd2019eng_education',
                             'imd2019eng_health',
                             'imd2019eng_crime',
                             'imd2019eng_barriers',
                             'imd2019eng_environment'))
#Welsh IMD: create quintiles for each domain and overall IMD score
welsh$imd2019wal<- welsh$WIMD2019_Quintile
welsh$imd2019wal_income<- ntile(welsh$Income_Rank,5)
welsh$imd2019wal_employment<-ntile(welsh$Employment_Rank,5)
welsh$imd2019wal_education<-ntile(welsh$Education_Rank,5)
welsh$imd2019wal_health<-ntile(welsh$Health_Rank,5) 
welsh$imd2019wal_safety<- ntile(welsh$CommunitySafety_Rank,5) 
welsh$imd2019wal_services<- ntile(welsh$AccessServices_Rank,5) 
welsh$imd2019wal_environment<- ntile(welsh$PhysicalEnvironment_Rank,5)  
welsh$imd2019wal_housing<- ntile(welsh$Housing_Rank,5) 

#Drop columns not needed in Welsh IMD
welsh<- subset(welsh, select=c('Granularity', 'imd2019wal',
                               'imd2019wal_income',
                               'imd2019wal_employment',
                               'imd2019wal_education',
                               'imd2019wal_health',
                               'imd2019wal_safety',
                               'imd2019wal_services',
                               'imd2019wal_environment',
                               'imd2019wal_housing'))
#Join together English and Welsh IMD
data<- full_join(english, welsh, by='Granularity')

#Scottish IMD: create quintiles for each domain and overall IMD score
scottish$imd2020scot<- scottish$SIMD2020v2_Quintile
scottish$imd2020scot_income<- ntile(scottish$SIMD2020v2_Income_Domain_Rank,5)
scottish$imd2020scot_employment<- ntile(scottish$SIMD2020_Employment_Domain_Rank,5)
scottish$imd2020scot_health<- ntile(scottish$SIMD2020_Health_Domain_Rank,5)
scottish$imd2020scot_education<- ntile(scottish$SIMD2020_Education_Domain_Rank,5)

scottish$imd2020scot_access<- ntile(scottish$SIMD2020_Access_Domain_Rank,5)
scottish$imd2020scot_crime<- ntile(scottish$SIMD2020_Crime_Domain_Rank,5)
scottish$imd2020scot_housing<- ntile(scottish$SIMD2020_Housing_Domain_Rank,5)

#Drop columns not needed in Scottish IMD
scottish<- subset(scottish, select= c('Granularity',
                                      'imd2020scot',
                                      'imd2020scot_income',
                                      'imd2020scot_employment',
                                      'imd2020scot_health',
                                      'imd2020scot_education',
                                      'imd2020scot_access',
                                      'imd2020scot_crime',
                                      'imd2020scot_housing'
))


#Join English and Welsh IMD to Scottish IMD
data<- full_join(data, scottish, by='Granularity')

#Irish IMD: create quintiles for each domain and overall IMD score
irish$imd2017ir<- ntile(irish$MDM_rank, 5)
irish$imd2017ir_income<- ntile(irish$D1_Income_rank, 5)
irish$imd2017ir_employment<- ntile(irish$D2_Empl_rank, 5)
irish$imd2017ir_health<- ntile(irish$D3_Health_rank, 5)
irish$imd2017ir_education<- ntile(irish$P4_Education_rank, 5)
irish$imd2017ir_access<- ntile(irish$P5_Access_rank, 5)
irish$imd2017ir_environment<- ntile(irish$D6_LivEnv_rank, 5)
irish$imd2017ir_crime<- ntile(irish$D7_CD_rank, 5)

#Drop columns not needed in Irish IMD
irish<- subset(irish, select=c('Granularity', 
                               'imd2017ir', 
                               'imd2017ir_income',
                               'imd2017ir_employment',
                               'imd2017ir_health',
                               'imd2017ir_education',
                               'imd2017ir_environment',
                               'imd2017ir_access',
                               'imd2017ir_crime'
))

#Join English, Welsh and Scottish IMD to Irish IMD
data<- full_join(data, irish, by='Granularity')

#CDRC's Combined  IMD: create quintiles for each domain and overall IMD score
combined$imd2019har<- ntile(combined$Rank, 5)
combined<- subset(combined, select=c('Granularity', 
                                     'imd2019har'))

data<- full_join(data, combined, by='Granularity')


#population estimates Scotland
scot_pop<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/Scottish IMD 2020/SAPE_SYOA_All_2021.csv')
#Aggregate population estimates by data zone
scot_pop <- scot_pop %>%
  group_by(FeatureCode) %>%
  summarize(MPE = sum(Value))
#Rename data zone column 
colnames(scot_pop)[1]<- c('Granularity')

#population estimates England and Wales
eng_pop<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/English IMD 2019/English population estimates lsoa mid 2020.csv')
#Rename data zone column and population estimate column
colnames(eng_pop)[1]<- c('Granularity')
colnames(eng_pop)[2]<- c('MPE')
#Drop columns that are not needed
eng_pop<- subset(eng_pop, select=c('Granularity','MPE'))
#Change MPE from character to numeric 
eng_pop$MPE <- gsub(",", "", eng_pop$MPE)
eng_pop$MPE <- as.numeric(eng_pop$MPE)


#population estimates Ireland
ire_pop<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/IMD/Northern Irish MDM 2017/Northern Ireland Population estimates mid 2020.csv')
#Rename SOA column and population estimate column
colnames(ire_pop)[1]<- c('Granularity')
colnames(ire_pop)[2]<- c('MPE')

#add in column to identify dataset
scot_pop$data_source<- 'MPE_scot_2021'
eng_pop$data_source<- 'MPE_eng_2020'
ire_pop$data_source<- 'MPE_ire_2020'

#combine the national population estimates into one file
mpe<- rbind(scot_pop, eng_pop,ire_pop)

#Transform MPE to categorical variables
# Define custom break points
break_points <- c(-Inf, 1200, 1500, 1600, 1800, Inf)
#Create labels
labels = c("<1200", "1200-1500", "1500-1600", "1600-1800", ">1800")
#Cut variable into 5 and add labels
mpe <- mpe %>%
  mutate(category = cut(MPE, breaks = break_points, labels = labels))
#Drop columns that are not needed
mpe<- subset(mpe, select = c('Granularity','category','data_source'))

#Join MPE data to IMD data
data_mpe<- full_join(data,mpe)

#split into 4 nations and relabel population estimates to include year
england<- subset(data_mpe, select=c('Granularity',
                                    "imd2019eng",             "imd2019eng_income",     
                                    "imd2019eng_employment",  "imd2019eng_education",   "imd2019eng_health",     
                                    "imd2019eng_crime",       "imd2019eng_barriers",    "imd2019eng_environment",
                                    "imd2019har","category"))
england<- england[!is.na(england$imd2019eng),]
colnames(england)[11]<- c('MPE_eng_2020')



wales<- subset(data_mpe, select=c('Granularity',
                                  "imd2019wal",             "imd2019wal_income",
                                  "imd2019wal_employment",
                                  "imd2019wal_education",   "imd2019wal_health",      "imd2019wal_safety",     
                                  "imd2019wal_services",    "imd2019wal_environment", "imd2019wal_housing",
                                  "imd2019har","category"))
wales <- wales[!is.na(wales$imd2019wal), ]
#rename mpe column
colnames(wales)[12]<- c('MPE_wal_2020')




scotland<- subset(data_mpe, select=c('Granularity',
                                     
                                     "imd2020scot",            "imd2020scot_income",     "imd2020scot_employment",
                                     "imd2020scot_health",     "imd2020scot_education",  "imd2020scot_access",    
                                     "imd2020scot_crime",      "imd2020scot_housing",
                                     "imd2019har","category"))
scotland<- scotland[!is.na(scotland$imd2020scot),]
colnames(scotland)[11]<- c('MPE_scot_2021')


ireland<- subset(data_mpe, select=c('Granularity',
                                    "imd2017ir",             
                                    "imd2017ir_income",       "imd2017ir_employment",   "imd2017ir_health",      
                                    "imd2017ir_education",    "imd2017ir_access",       "imd2017ir_crime","imd2017ir_environment",  
                                    "imd2019har","category"))

ireland<- ireland[!is.na(ireland$imd2017ir),]
colnames(ireland)[11]<- c('MPE_ire_2020')


#Add in urban- rural classification

#England and wales urban rural classification
#Import england ur file
ur_eng<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/urb_rur/Rural_Urban_Classification_(2011)_of_Lower_Layer_Super_Output_Areas_in_England_and_Wales.csv')
# recode - keep letter component only - this is to minimise disc risk where the sparse settting component is included

#select letter only
ur_eng$RUC11CD<- substr(ur_eng$RUC11CD,1,1)

# Define a named vector for letter-to-number mapping
letter_to_number <- setNames(1:26, LETTERS)

# Function to convert a single letter to its corresponding number
convert_letter_to_number <- function(letter) {
  letter <- toupper(letter)
  if (letter %in% names(letter_to_number)) {
    return(letter_to_number[letter])
  } else {
    return(NA)
  }
}

# Apply the conversion function to the entire column
ur_eng$ur_eng_wal_2011 <- sapply(ur_eng$RUC11CD, convert_letter_to_number)
#rename lsoa column
colnames(ur_eng)[1]<- c('Granularity')

#drop columns not needed
ur_eng_wal<- subset(ur_eng, select=c('Granularity','ur_eng_wal_2011'))

#add urban-rural classification to england dataset
england<- left_join(england, ur_eng_wal)
#add urban-rural classification to welsh dataset
wales<- left_join(wales, ur_eng_wal)


#Read in the scottish urban-rural classification
ur_scot<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/urb_rur/datazone2011_urban_rural_2020_15092022.csv')
#rename the data zone column and urban rural classification
colnames(ur_scot)[1]<- c('Granularity')
ur_scot$ur_scot_2020<- ur_scot$UrbanRural6fold2020

#drop columns that are not needed
ur_scot<- subset(ur_scot, select=c('Granularity', 'ur_scot_2020'))
#Join scottish urban rural data to scottish dataset
scotland<- full_join(scotland, ur_scot)

#Ireland urban rural classification load in data
ur_ire<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/urb_rur/Rural_Urban_Classification_Northern_Ireland_2015.csv')
#Change from categorical to numeric
ur_ire$ur_ire_2015<- factor(ur_ire$X2015.Default.Urban.Rural,levels=c('Urban','Mixed urban/rural','Rural'))
ur_ire$ur_ire_2015<- as.numeric(ur_ire$ur_ire_2015)
#Rename SOA column and urban rural classification column
colnames(ur_ire)[1]<- c('Granularity')

#Drop columns not needed
ur_ire<- subset(ur_ire, select=c('Granularity', 'ur_ire_2015'))



#join together urban-rural ireland data and ireland dataset
ireland<- full_join(ireland, ur_ire)

#Add in country and region
#Add in country to england
england$Country<- 'England'
#Add in region, read in look up table
eng_reg<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/lookups/Lower_Layer_Super_Output_Area_(2011)_to_Built-up_Area_Sub-division_to_Built-up_Area_to_Local_Authority_District_to_Region_(December_2011)_Lookup_in_England_and_Wales.csv')
#subset and remove duplicates
eng_reg<- subset(eng_reg, select=c('LSOA11CD','RGN11NM'))
eng_reg<- eng_reg[!eng_reg$RGN11NM=='Wales',]
#Change column names
colnames(eng_reg)[1]<- c('Granularity')

#add regions to england dataset
england<- full_join(england,eng_reg)

#Adding in regions to Wales, Scotland and Northern Ireland is too disclosive
#Add in Country label
scotland$Country<- 'Scotland'
ireland$Country<- 'Ireland'
wales$Country<- 'Wales'
#Change variables to lowercase
colnames(england)[1]<- 'granularity'
colnames(scotland)[1]<- 'granularity'
colnames(wales)[1]<- 'granularity'
colnames(ireland)[1]<- 'granularity'
colnames(england)[13]<- 'country'
colnames(wales)[14]<- 'country'
colnames(scotland)[13]<- 'country'
colnames(ireland)[13]<- 'country'

#Add in harmonised Urban Rural Classification
ur_harm<- read.csv('//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/urb_rur/uk_ruc_2021.csv')
#Change column names
colnames(ur_harm)[1]<-c('granularity')
colnames(ur_harm)[3]<-c('ur_harm_2021')
#Subset variables to use the 3 tier classification of urban-rural
ur_harm<- subset(ur_harm, select=c('granularity','ur_harm_2021'))
#Transform the variables from 0,1,2 to 1,2,3
ur_harm$ur_harm_2021<- ifelse(ur_harm$ur_harm_2021==2,3,ur_harm$ur_harm_2021)
ur_harm$ur_harm_2021<- ifelse(ur_harm$ur_harm_2021==1,2,ur_harm$ur_harm_2021)
ur_harm$ur_harm_2021<- ifelse(ur_harm$ur_harm_2021==0,1,ur_harm$ur_harm_2021)

#Join with original country data
england<- left_join(england,ur_harm)
wales<- left_join(wales,ur_harm)
scotland<- left_join(scotland,ur_harm)
ireland<- left_join(ireland,ur_harm)


colnames(england)[1]<- 'geo_unit'
colnames(wales)[1]<- 'geo_unit'
colnames(scotland)[1]<- 'geo_unit'
colnames(ireland)[1]<- 'geo_unit'


#Save 4 nations datasets
setwd("//ads.bris.ac.uk/filestore/BRMS/Studies/LLC/Data/geo_data/geo_CORE")
write.csv(england, 'CORE_geo_indicators_england_v0001_20240614.csv')
write.csv(wales, 'CORE_geo_indicators_wales_v0001_20240614.csv')
write.csv(scotland, 'CORE_geo_indicators_scotland_v0001_20240614.csv')
write.csv(ireland, 'CORE_geo_indicators_ireland_v0001_20240614.csv', append=T)

