from powerservice import trading
import pandas as pd
import numpy as np
from datetime import datetime
import csv

# Importing the relevant datasets
trades_today = trading.get_trades(date='14/09/2022')
trades_yesterday = trading.get_trades(date='13/09/2022')

initial = pd.DataFrame(trades_today[0])
# print(initial)


# Aggregating the data
times = pd.to_datetime(initial.time)
aggregated = initial.groupby([times.dt.hour]).agg(date=('date', 'max'), volume=("volume", "sum"),
                                                         id=('id', 'max'))
aggregated = aggregated.reset_index()[['date', 'time', 'volume', 'id']]


# Selecting for the required columns
time_volume = aggregated[["time", "volume"]]
# print(time_volume)

# Collecting the values from yesterday
previous = pd.DataFrame(trades_yesterday[0])
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

datetime_series = pd.to_datetime(initial['time'])
datetime_index = pd.DatetimeIndex(datetime_series.values)
df3 = initial.set_index(datetime_index)

df4 = df3.resample('5T').mean()

df3 = df3.set_index(df4.index)
df3 = df3.set_index(df4.index).reset_index()
df3 = df3.fillna(0)
df_quality1 = df3[df3['time'] == 0]
df_quality1['index'] = df_quality1['index'].dt.strftime('%H:%M')

df_quality1.rename(columns={'index': 'missed_intervals'}, inplace=True)
df_quality1 = df_quality1[["date", "missed_intervals", "id"]]

fields = ['<< TIME INTERVAL CHECK >>']
with open(file_name, 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)

# Exporting Data Quality CSV
df_quality1.to_csv(file_name, mode='a', index=False)

df_quality2 = df3[(df3['volume'] == 0) & (df3['time'] != 0)]
df_quality2 = df_quality2[["date", "time", "volume", "id"]]

fields = ['<< MISSING VALUES CHECK >>']
with open(file_name, 'a') as f:
    writer = csv.writer(f)
    writer.writerow(fields)

# Exporting Data Quality CSV
df_quality2.to_csv(file_name, mode='a', index=False)

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
