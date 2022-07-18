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
    '''
    Parameters
    ----------
    df : dataframe with values to operate on
    var_to_cut : list of variables in dataframe to operate on
    new_var_name : new variable name once processed
    cut_num : quantile/cut value

    Returns
    -------
    df : returns input dataframe with new variables added

    '''
    # create var name
    v = str(new_var_name) + '_q' + str(cut_num)
    print(v)
    df[v] = pd.qcut(df[var_to_cut], cut_num, labels=False) + 1 
    return df

# incremental list generator
def incremental_range(start, stop, step):
    '''
    Parameters
    ----------
    start : numeric start of range
    stop : numeric end of range
    step : numeric step between 

    Yields
    ------
    value : returned back to function
    
    '''
    value = start
    while value < stop:
        yield value
        value += step

# data file locations
dataloc = "L:/Data/geo_data"

# 1a) IMD -  import IMD with subdomains
imd19sub = pd.read_excel(dataloc+"/IMD/File_2_-_IoD2019_Domains_of_Deprivation.xlsx", sheet_name="IoD2019 Domains")

# CREATE QUANTILES
# define cuts (quantiles)
cuts = [3,4,5]
# create list of decile vars to operate on as these are included in the dataset already 
imd19sub_cols = imd19sub.columns.values.tolist()
# create list of rank vars to drop
rank = [x for x in imd19sub_cols if "Rank" in x]
# define new rank var names
imd_vars_ls = ['imd2019', 'imd2019_income', 'imd2019_employment', 'imd2019_education',
            'imd2019_health','imd2019_crime','imd2019_barriers', 'imd2019_environment']
# put old and new var names together in list
imd_vars = dict(zip(rank, imd_vars_ls))
# derive quintiles, quartiles, tertiles
for a in cuts:
    for k, v in imd_vars.items():
        make_quantiles(imd19sub, k, v, a)
# value counts check
for i in cuts:
    print(imd19sub['imd2019_q'+str(i)].value_counts())

# ISOLATE DECILES
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
    
# CLEARUP AND STRIP DOWN DF
# create list of rank vars to drop
rank = [x for x in imd19sub_cols if "Rank" in x]
# other columns, combine with rank vars
to_drop = ['Local Authority District code (2019)', 'Local Authority District name (2019)','LSOA name (2011)'] + rank
# drop unrequired cols
imd19sub = imd19sub.drop(columns=to_drop)

###################
# 2) pop density
###################
pop_den = pd.read_excel(dataloc+"/pop_den/sape23dt11mid2020lsoapopulationdensity.xlsx", sheet_name="Mid-2020 Population Density", skiprows = 4)
# hist to assess distributio
pop_den.hist(column='People per Sq Km')
# cut up col
# first create incremental list
t1 = list(incremental_range(0, 10100, 100))
# then create catagorical variable as string
pop_den['people_km2_mid2020'] = pd.cut(pop_den['People per Sq Km'], bins=t1).astype(str)
# add topcut
pop_den['people_km2_mid2020'] = np.where(pop_den['People per Sq Km']>=10000,">=10000",pop_den['people_km2_mid2020'])
# value counts
t2 = pop_den['people_km2_mid2020'].value_counts()
# strip down vars
pop_den = pop_den[['LSOA Code','people_km2_mid2020']]
# merge with IMD file
all_geo = imd19sub.merge(pop_den, how='right', left_on='LSOA code (2011)', right_on='LSOA Code')

#####################
# 3) urban rural
#####################
# 3a) data
urb_rur = pd.read_csv(dataloc+"/urb_rur/Rural_Urban_Classification_(2011)_of_Lower_Layer_Super_Output_Areas_in_England_and_Wales.csv")
# recode - keep letter component only - this is to minimise disc risk where the sparse settting component is included
urb_rur['RUC11CD_V2'] = urb_rur['RUC11CD'].str[0]
urb_rur['RUC11CD_V2'].value_counts()
# strip down actual data dropping unrequireed vars
urb_rur = urb_rur[['LSOA11CD', 'RUC11CD_V2']]
# merge with other geo data
all_geo = all_geo.merge(urb_rur, how='inner', left_on='LSOA code (2011)', right_on='LSOA11CD')


# 3b) metadata
# created collapse df which can be used to generate metadata table
urb_rur_meta = urb_rur[['RUC11', 'RUC11CD_V2']].drop_duplicates(subset='RUC11CD_V2', keep='last')
# update metadata to reflect collapse of catagories 
urb_rur_meta['RUC11'] = np.where(urb_rur_meta['RUC11CD_V2']=='C','Urban city and town (incl. in sparse setting)',
                                 np.where(urb_rur_meta['RUC11CD_V2']=='D', 'Rural town and fringe (incl. in a sparse setting)',
                                 np.where(urb_rur_meta['RUC11CD_V2']=='E', 'Rural village and dispersed (incl. in a sparse setting)',
                                          urb_rur_meta['RUC11'])))
# rename cols to metadata convention
urb_rur_meta.rename({'RUC11' : 'variable_label',
                     'RUC11CD_V2' : 'value_value'}, axis=1, inplace = True)
# create missing columns
urb_rur_meta['table_name'] = 'geodata_nhs_geo_indicators'
urb_rur_meta['variable_name'] = 'RUC11CD_V2'
# assign to values - vars required : table_name, variable_name, value_value, value_label
urb_rur_vals = urb_rur_meta
# create descriptions table - vars required: table_name, variable_name, variable_label
urb_rur_desc = urb_rur_meta[['table_name', 'variable_name']].drop_duplicates()
# add variable label 
urb_rur_desc['variable_label'] = '2011 Urban rural classification collapsed from 8 catogories to 5'

####################
# 4) Region
####################
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





