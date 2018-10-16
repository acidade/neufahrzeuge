import pandas as pd
import time
import datetime

# start timestamp for csv file to make sure to have a unique filename when saving
time_start = time.time()
file_timestamp = datetime.datetime.fromtimestamp(time_start).strftime('%Y%m%d%H%M%S')

# define filename for final file and csv separator
file_name = 'new-vehicles-all-years'
file_raw = 'RAW'
file_processed = 'PRO'
file_final = 'FIN'
file_format = '.csv'
file_separator = ','

##############################################
################# pull data ##################
##############################################

# define base-url (auto.swiss) and files to pull
base_url = 'https://www.auto.swiss/fileadmin/3_Statistiken/Autoverkaeufe_nach_Modellen/'
filenames = ['ModellePW2012.xls', 'ModellePW2013.xls','ModellePW2014.xls','ModellePW2015.xlsx','ModellePW2016.xlsx','ModellePW2017.xlsx','ModellePW2018.xlsx']

# define dataframe to collect all data
df_alle = pd.DataFrame()

# loop through all files
for file in filenames:
    file_in = base_url+file
    # catch possible naming of months
    monthnames = {'Jan':'01','Feb':'02','Mär':'03','Mrz':'03','Mar':'03','Apr':'04','Mai':'05','Jun':'06','Jul':'07','Aug':'08','Sep':'09','Okt':'10','Nov':'11','Dez':'12'}

    # get year from Excel file (G7), this might change in the future
    year = pd.read_excel(file_in, header=None, usecols='G', skiprows=6, nrows=1, squeeze=True)
    year = year[0]
    
    # define temporary df for all entries from year n
    year_df = pd.DataFrame()

    # get all relevant data from Excel file (All sheets, A16:C(n-1))
    df_xls = pd.read_excel(file_in, sheet_name=None, header=15, usecols='A:C', skipfooter=1)
    
    # loop through all the months of the received data
    for dfs in df_xls:
        # set column 'Monat' to number of the month
        df_xls[dfs]['Monat'] = monthnames[dfs[:3]]
        # set column 'Jahr' to number of the year
        df_xls[dfs]['Jahr'] = year
        # connect all the monthly data to a yearly df
        year_df = year_df.append(df_xls[dfs], ignore_index=True)
    
    # connect all the yearly dfs to a total df
    df_alle = df_alle.append(year_df, ignore_index=True)
    print(f'{year} done.')

#print(df_alle)

# save all data to 'file_out_raw'
try:
    savefile = str(file_timestamp)+'_'+file_name+'_'+file_raw+file_format
    df_alle.to_csv(savefile, sep=file_separator, index=False)
    print(f'File "{savefile}" was written to disk.')
except:
    print('There was an error while writing the data to disk.')


##############################################
################ format data #################
##############################################

df = pd.read_csv(savefile, dtype=str)

# add column 'model_id' to all rows (Marke+Modell)
df['model_id'] = df.apply(lambda row: str(row.Marke) + str(row.Modell), axis=1)

# count unique years, brands and models
years = df['Jahr'].unique()
brands = df['Marke'].unique()
models = df['Modell'].unique()
print(f'{len(brands)} brands and {len(models)} models found from {len(years)} years in file "{savefile}".')

# create dictionary for the years-loop
year_dict = dict()
yearcount = 0

# create dictionary for the year-data
df_year = {}
df_year[0] = []

# initiate final DataFrame
final = pd.DataFrame()

# loop through all years
for year in years:
    # put all years in a dictionary (we need the years to build the column names)
    year_dict[year] = df.loc[df['Jahr'] == year]

    # count all the months in the year
    months = year_dict[year]['Monat'].unique()

    # create dictionary for the months
    month_dict = dict()
    monthcount = 1
  
    # loop through months
    for month in months:
    # put all months in a dict (we need months for col names)
        month_dict[month] = year_dict[year].loc[year_dict[year]['Monat'] == month]

        # get month and year that we're working on
        jahr = month_dict[month].iloc[0]['Jahr']
        monat = month_dict[month].iloc[0]['Monat']

        # remove columns 'Monat' and 'Jahr', so we can merge easier
        df_dropped = month_dict[month].drop(columns=['Jahr','Monat'])

        # rename the column with n of models from 'Anzahl' to e.g. '2018-01_SUM'
        df_renamed = df_dropped.rename(index=str, columns={'Anzahl': jahr+'-'+monat+'_SUM'})
        print(f'{year}-{month} done')
    
        if monthcount == 1:
            # if it's the first month and year, re-order the columns
            df_year[yearcount] = df_renamed[['model_id','Marke','Modell', jahr+'-'+monat+'_SUM']]
        else:
            # else, just merge the month with the yearly df
            df_year[yearcount] = pd.merge(df_year[yearcount], df_renamed, how='outer', on=['model_id', 'Marke', 'Modell'])
        monthcount += 1
    # replace all the NaN with 0's, because 0 cars were sold
    df_year[yearcount] = df_year[yearcount].fillna(0)

    df_calc = df_year[yearcount].iloc[:,3:].astype(int)
    df_calc = df_calc.diff(axis=1)
    df_calc.iloc[:,0:1] = df_year[yearcount].iloc[:,3:4]
    df_calc.rename(columns=lambda x: x[:-3]+'ABS', inplace=True)
    df_year[yearcount] = pd.concat([df_year[yearcount], df_calc], axis=1)

    yearcount += 1

# Prefill final-DF with index columns
final = df_year[0].iloc[:,:3]

# Merge final with yearly dfs
for k, v in df_year.items():
    final = pd.merge(final, v, how='outer', on=['model_id', 'Marke', 'Modell'])

# replace NaN by 0
final = final.fillna(0)
# replace negative values by 0
num = final._get_numeric_data()
num[num < 0] = 0

# save all data to file
try:
    savefile = str(file_timestamp)+'_'+file_name+'_'+file_processed+file_format
    final.to_csv(savefile, sep=file_separator, index=False)
    print(f'File "{savefile}" was written to disk.')
except:
    print('There was an error while writing the data to disk.')
