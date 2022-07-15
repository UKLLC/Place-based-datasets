##########################################################
# purpose: create geo indicator file for upload to SeRP
# author: rich thomas
# date: 08/07/2022
##########################################################

# modules
import pandas as pd
import numpy as np

# functions
# quantile maker
def make_quantiles(df, var_to_cut, new_var_name, cut_num):
    # create var name
    v = str(new_var_name) + '_q' + str(cut_num)
    print(v)
    df[v] = pd.qcut(df[var_to_cut], cut_num, labels=False) + 1 
    return df
# incremental list generator
def incremental_range(start, stop, step):
    value = start
    while value < stop:
        yield value
        value += step

# file locations
dataloc = "L:/Data/geo_data"
# 1a) import IMD with subdomains
imd19sub = pd.read_excel(dataloc+"/IMD/File_2_-_IoD2019_Domains_of_Deprivation.xlsx", sheet_name="IoD2019 Domains")
# define cuts
cuts = [3,4,5]
# define vars to operate on with new vars names in dict
imd_vars = {'Index of Multiple Deprivation (IMD) Rank (where 1 is most deprived)': 'imd2019',
            'Income Rank (where 1 is most deprived)': 'imd2019_income',
            'Employment Rank (where 1 is most deprived)': 'imd2019_employment',
            'Education, Skills and Training Rank (where 1 is most deprived)': 'imd2019_education',
            'Health Deprivation and Disability Rank (where 1 is most deprived)': 'imd2019_health',
            'Crime Rank (where 1 is most deprived)': 'imd2019_crime',
            'Barriers to Housing and Services Rank (where 1 is most deprived)': 'imd2019_barriers',
            'Living Environment Rank (where 1 is most deprived)': 'imd2019_environment',
            }

# derive quintiles, quartiles, tertiles
for a in cuts:
    for k, v in imd_vars.items():
        make_quantiles(imd19sub, k, v, a)

# value counts check
for i in cuts:
    print(imd19sub['imd2019_q'+str(i)].value_counts())
# create list of decile vars to operate on
imd19sub_cols = imd19sub.columns.values.tolist()
# extract deciles vars
decs = [x for x in imd19sub_cols if "Decile" in x]
# define new var names 
decs_new = list(imd_vars.values())
decs_new = [x + "_q10" for x in decs_new]
# convert to dict
decs_d = dict(zip(decs, decs_new))
# rename
for k, v in decs_d.items():
    imd19sub.rename({k:v}, axis=1, inplace = True)
# create list of rank vars to drop
rank = [x for x in imd19sub_cols if "Rank" in x]
# other columns, combine with rank vars
to_drop = ['Local Authority District code (2019)', 'Local Authority District name (2019)','LSOA name (2011)'] + rank
# drop unrequired cols
imd19sub = imd19sub.drop(columns=to_drop)

# 2) pop density
pop_den = pd.read_excel(dataloc+"/pop_den/sape23dt11mid2020lsoapopulationdensity.xlsx", sheet_name="Mid-2020 Population Density", skiprows = 4)
# hist to assess distributio
pop_den.hist(column='People per Sq Km')
# cut up col
# first create incremental list
t1 = list(incremental_range(0, 10100, 100))
# then create catagorical variable
pop_den['people_km2_mid2020'] = pd.cut(pop_den['People per Sq Km'], bins=t1)
# value counts check
t2 = pop_den['people_km2_mid2020'].value_counts()
# merge with IMD file
all_geo = imd19.merge(pop_den, how='right', left_on='LSOA code (2011)', right_on='LSOA Code')

# 3) urban rural
urb_rur = pd.read_csv(dataloc+"/urb_rur/Rural_Urban_Classification_(2011)_of_Lower_Layer_Super_Output_Areas_in_England_and_Wales.csv")
# check counts of urb rur codes - maybe need to look at collapsing those in sparse setting
urb_rur['RUC11CD'].value_counts()
# drop unrequireed vars
urb_rur = urb_rur.drop(columns=['FID'])
# merge with other geo data
all_geo = all_geo.merge(urb_rur, how='inner', left_on='LSOA code (2011)', right_on='LSOA11CD')
# test to look at areas which have codes with low counts
#t1 = urb_rur.loc[urb_rur['RUC11CD']=='C2']

# 4) Region
# bring in lookups
t1 = pd.read_csv(dataloc+"/lookups/Lower_Layer_Super_Output_Area_(2011)_to_Ward_(2017)_Lookup_in_England_and_Wales.csv")
t2 = pd.read_csv(dataloc+"/lookups/Ward_to_Local_Authority_District_to_County_to_Region_to_Country_(December_2017)_Lookup_in_United_Kingdom_version_2.csv")
t3 = t1.merge(t2, how='inner', on='WD17CD')
# value counts
t3['GOR10NM'].value_counts()
# drop unrequired vars
regions = t3[['LSOA11CD', 'GOR10CD', 'GOR10NM', 'CTRY17CD', 'CTRY17NM']]
# merge with other geo files
all_geo = all_geo.merge(regions, how='left', on='LSOA11CD')
# clearup
all_geo  = all_geo.drop(columns=['LSOA code (2011)', 'LSOA Code', 'LSOA name (2011)', 'LSOA Name'])

# dump out as csv
all_geo.to_csv(dataloc+"/data_processing/geo_file2/Linkage_lsoa_geo_data.csv", index=False)

# generate metadata





