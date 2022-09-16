from powerservice import trading
import pandas as pd
import numpy as np
from datetime import datetime
import csv

# Importing the relevant datasets
trades_today = trading.get_trades(date='14/09/2022')
trades_yesterday = trading.get_trades(date='13/09/2022')

initial = pd.DataFrame(trades_today)
# print(initial)

# Flattening the nested values within the time and volume columns
flat_initial = initial.explode(['time', 'volume'])
# print(flat_initial.head(40))
flat_initial = flat_initial.reset_index()[['date', 'time', 'volume', 'id']]
# print(flat_initial)
flat_initial["volume"] = flat_initial.volume.astype(float)
flat_initial1 = flat_initial
# print(flat_initial1.head(30))


# Aggregating the data
times = pd.to_datetime(flat_initial.time)
flat_initial = flat_initial.groupby([times.dt.hour]).agg(date=('date', 'max'), volume=("volume", "sum"),
                                                         id=('id', 'max'))
flat_initial = flat_initial.reset_index()[['date', 'time', 'volume', 'id']]
# print(flat_initial)

# Selecting for the required columns
time_volume = flat_initial[["time", "volume"]]
# print(time_volume)

# Collecting the values from yesterday
previous = pd.DataFrame(trades_yesterday)
# Flattening the nested values within the time and volume columns
previous = previous.explode(['time', 'volume'])
# print(previous)
previous = previous.reset_index()[['date', 'time', 'volume', 'id']]
# print(previous)

# Selecting the values from 23:00
previous['time'] = pd.to_datetime(previous['time'])
start_time = previous.set_index('time').between_time('23:00', '23:59')
start_time = start_time.reset_index()[['date', 'time', 'volume', 'id']]

# Aggregating the values
times2 = pd.to_datetime(start_time.time)
start_time = start_time.groupby([times2.dt.hour]).agg(date=('date', 'max'), volume=("volume", "sum"), id=('id', 'max'))
start_time = start_time.reset_index()[['date', 'time', 'volume', 'id']]
# Selecting for the required columns
start_t_v = start_time[["time", "volume"]]

# Moving the 23:00 values to the beginning of the DataFrame
from_start = pd.concat([start_t_v, time_volume])
# print(from_start)

# Converting time elements to 24-hour format
from_start['time'] = pd.to_datetime(from_start.time, format='%H')
from_start['time'] = from_start['time'].dt.strftime('%H:%M')
# print(from_start)

# Dropping the second 23:00 value, resetting the index and deleting the old index column
from_start = from_start.drop(23)
from_start = from_start.reset_index()
from_start = from_start.drop(columns="index")
# print(from_start)

# Exporting the CSV file in the required format
now = datetime.now()
dt_string = now.strftime("%Y%m%d_%H%M")
# print(dt_string)
from_start.to_csv('PowerPosition_' + dt_string + '.csv', index=False)

# Exporting the Data Profiling CSV
data_profile = [('PowerPosition_' + dt_string + '.csv')]
with open('PowerPosition_' + dt_string + '_data_profiling' + '.csv', 'w', newline='') as csvfile:
    my_writer = csv.writer(csvfile, delimiter=' ')
    my_writer.writerow(data_profile)

file_name = 'PowerPosition_' + dt_string + '_data_quality.csv'

if from_start.time[0] == '23:00' and from_start.time[23] == '22:00':
    fields = ['<< START AND END TIMES ARE VALID >>']
else:
    fields = ['<< START AND END TIMES ARE INVALID >>']
with open(file_name, 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)

if pd.to_datetime(from_start['time'], format='%H:%M', errors='coerce').notnull().all():
    fields = ['<< TIME COLUMN VALUES HAVE VALID FORMAT >>']
else:
    fields = ['<< TIME COLUMN VALUES HAVE INVALID FORMAT >>']
with open(file_name, 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)

flat_initial1['time'] = pd.to_datetime(flat_initial1['time'])
flat_initial1 = flat_initial1.set_index('time')

df1 = (flat_initial1.groupby([pd.Grouper(freq='D'), 'id', 'date'])['volume']
       .apply(lambda x: x.asfreq('5min'))
       .reset_index(level=0, drop=True)
       .reset_index())
df1 = df1.reset_index()[['date', 'time', 'volume', 'id']]
df1['time'] = df1['time'].dt.strftime('%H:%M')
df2 = df1[df1['volume'].isna()]
df3 = df1.fillna('0')

fields = ['<< MISSING TIME INTERVALS: >>']
with open(file_name, 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)

# export csv file
df2.to_csv(file_name, mode='a', index=False)

df_quality = df3[(df3['volume'] == 0) & (df3['time'] != 0)]
df_quality = df_quality[["date", "time", "volume", "id"]]

fields = ['<< MISSING VALUES CHECK >>']
with open(file_name, 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)

# Exporting Data Quality CSV
df_quality.to_csv(file_name, mode='a', index=False)
