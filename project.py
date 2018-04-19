# the four turnstile data sets used are the full week datasets for each week in May 2017
# we wanted to use the most similar data to the time we would be advertising given the conference is in the summer

# this is the process we went through to optimize results:
# 1. all the data for May 2017 was compiled to be the most representative sample
# 2. the master dataframe is reduced to only stations we determined are tech hubs based on research
# 3. the tech dataframe is split into weekend and weekday dataframes
# 4.

import pandas as pd
from pprint import pprint


def main():
    master_df = read_csv("turnstile_170506.txt", "turnstile_170513.txt", "turnstile_170520.txt", "turnstile_170527.txt")
    weekday_df, weekend_df = split_dataframe(master_df)
    print(weekend_df)
    # pprint(busiest_stations(master_df))


def read_csv(file1, file2, file3, file4):
    # takes in: 4 CSV FILES
    # returns: PANDAS DATAFRAME

    # this function should read and compile four different csv files to get the optimal amount of data to work with

    # each function is converted to its own dataframe here:
    df1 = pd.read_csv(file1)  # 197290 entries
    df2 = pd.read_csv(file2)  # 196861 entries
    df3 = pd.read_csv(file3)  # 195475 entries
    df4 = pd.read_csv(file4)  # 195463 entries

    # the dataframes are then turned into a master dataframe here:
    df = pd.concat([df1, df2, df3, df4], ignore_index=True)  # 785089 entries
    # that means df compiled correctly

    return df


def split_dataframe(df):
    # takes in: ONE PANDAS DATAFRAME
    # returns: TWO PANDAS DATAFRAMES

    # this should take in a dataframe and return one dataframe with weekday data and one with weekend data

    df['DATE'] = pd.to_datetime(df.DATE)
    df['DATE'] = df.DATE.dt.dayofweek

    # dates are converted to days of week
    # monday=0 and sunday=6

    weekday_df = df[(df.DATE == 0) | (df.DATE == 1) | (df.DATE == 2) | (df.DATE == 3) | (df.DATE == 4)]
    weekday_df = weekday_df.reset_index(drop=True)

    weekend_df = df[(df.DATE == 5) | (df.DATE == 6)]
    weekend_df = weekend_df.reset_index(drop=True)

    return weekday_df, weekend_df


def busiest_stations(df):  # fls is the few data frames provided
    df.columns = df.columns.str.strip()
    time_entries_dict = {}
    for i in range(len(df.index)):
        station_name = (df.STATION[i])
        if station_name not in time_entries_dict.keys():
            time_entries_dict[station_name] = [(df['TIME'][i], df['ENTRIES'][i])]
        else:
            time_entries_dict[station_name].append((df['TIME'][i], df['ENTRIES'][i]))
    time_diff_dict = {}
    for key, value in time_entries_dict.items():
        for i in range(1, len(value)):
            if key not in time_diff_dict.keys():
                time_diff_dict[key] = [(df['TIME'][i], (df['ENTRIES'][i + 1]) - df['ENTRIES'][i])]
            else:
                time_diff_dict[key].append((df['TIME'][i], (df['ENTRIES'][i + 1]) - df['ENTRIES'][i]))
    final = {}
    for key, value in time_diff_dict.items():
        temp = 0
        for i in value:
            temp += i[1]
            final[key] = temp
    total_station = {}
    for key, value in final.items():
        new_key = key
        if new_key in total_station:
            total_station[new_key] += value
        else:
            total_station[new_key] = value
    fls = []
    for key, value in total_station.items():
        temp_tuple = (value, key)
        fls.append(temp_tuple)
    fls.sort(reverse=True)
    return fls

main()
