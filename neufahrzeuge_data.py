import pandas as pd
import time
import datetime


#####################################################################
###################### Start of pulling data ########################
#####################################################################

# start timestamp for csv file to make sure to have a unique filename when saving
time_start = time.time()
timestamp_file = datetime.datetime.fromtimestamp(time_start).strftime('%Y%m%d%H%M%S')

# define base-url (auto.swiss) and files to pull
base_url = 'https://www.auto.swiss/fileadmin/3_Statistiken/Autoverkaeufe_nach_Modellen/'
filenames = ['ModellePW2012.xls', 'ModellePW2013.xls','ModellePW2014.xls','ModellePW2015.xlsx','ModellePW2016.xlsx','ModellePW2017.xlsx','ModellePW2018.xlsx']

# define filename for final file and csv separator
file_out_raw = 'neufahrzeuge_alle_jahre_RAW'
csv_separator = ','

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

print(df_alle)

# save all data to 'file_out_raw'
try:
    savefile = str(timestamp_file)+'_'+file_out_raw+'.csv'
    df_alle.to_csv(savefile, sep=csv_separator, index=False)
    print(f'File "{savefile}" was written to disk.')
except:
    print('There was an error while writing the data to disk.')

#####################################################################
####################### Start of formatting #########################
#####################################################################

import pandas as pd

#define output and csv separator
file_out_formated = "neufahrzeuge_alle_jahre_PRO"
csv_separator = ","

# define input file
file_in = savefile

df = pd.read_csv(file_in, dtype=str)

# add column 'model_id' to all rows (Marke+Modell)
df['model_id'] = df.apply(lambda row: str(row.Marke) + str(row.Modell), axis=1)

# count unique years, brands and models
years = df['Jahr'].unique()
brands = df['Marke'].unique()
models = df['Modell'].unique()
print(f'{len(brands)} Marken und {len(models)} Modelle in {len(years)} Jahren gefunden')

# create dictionary for the years
year_dict = dict()
yearcount = 1

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
    
        if monthcount == 1 and yearcount == 1:
            # if it's the first month and year, re-order the columns
            final = df_renamed[['model_id','Marke','Modell', jahr+'-'+monat+'_SUM']]
        else:
            # else, just merge the month with the yearly df
            final = pd.merge(final, df_renamed, how='outer', on=['model_id', 'Marke', 'Modell'])
        monthcount += 1
    yearcount += 1

# replace all the NaN with 0's, because 0 cars were sold
final = final.fillna(0)

print(final)

# save all data to 'file_out_formated'
try:
    final.to_csv(str(timestamp_file)+'_'+file_out_formated+'.csv', sep=csv_separator, index=False)
    print(f'File "{timestamp_file}_{file_out_formated}.csv" was written to disk.')
except:
    print('There was an error while writing the data to disk.')
