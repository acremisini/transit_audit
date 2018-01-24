import pandas as pd
from datetime import datetime
import numpy as np
import json
import pymysql.cursors
import sys
sys.path.insert(0, '/home/transit/www/data/secure')
import database
from collections import OrderedDict

#usage <timespan_start=YYYY-MM-DD%HH:MM:SS&timespan_end=YYYY-MM-DD%HH:MM:SS&stats=Boolean&station_list=comma,separated,stations>
#note: this returns trips that both begin and end within the timespan, sorted for each station by time_announced

def connect_and_return_data(timespan_start, timespan_end):

    connection = pymysql.connect(host=database.host,
                                 user=database.user,
                                 password=database.password,
                                 db=database.database,
                                 unix_socket = database.unix_socket,
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            query = """
                SELECT * FROM trainTrip
                WHERE dateadded >= '{}' AND dateadded <= '{}'
                ORDER BY StationID, Time1_Train, dateadded;
                """.format(timespan_start, timespan_end)
            result = pd.read_sql_query(query, connection)
    finally:
        connection.close()
        return result

# filter out ghosts
def clean_train_trip(df):
    df_filter = pd.DataFrame()
    i = 0
    filter_list = list()
    l = len(df) - 1
    # df_filter = df.where(df['Time1_Est'] == 0 and df[''])
    while i < l:
        if int(df.at[i, 'Time1_Est']) != 0 and int(df.at[i + 1,'Time1_Est']) == 0 and int(df.at[i,'Time1_Train']) == int(df.at[i + 1,'Time1_Train']):
            filter_list.append(df.loc[i])
            filter_list.append(df.loc[i+1])
            i += 2
        # i know the first one in the tuple is ghost, so try to form tuple with following report
        else:
            i += 1
    df_filter = pd.concat(filter_list, axis=1)
    return df_filter

def make_data_dicts(df, stats, station_list):
    station_dict = dict()
    stats_dict = dict()
    headway_delta_stats = dict()
    i = 0
    #all tuples are good now
    while i < len(df.columns) - 1:
        try:
            if df[i]['StationID'] in station_list:
                if df[i]['StationID'] not in station_dict:
                    station_dict[str(df[i]['StationID'])] = pd.DataFrame(columns=['trip_num',
                                                                                 'time_announced',
                                                                                 'projected_duration',
                                                                                 'time_arrived',
                                                                                 'actual_duration',
                                                                                 'time_since_last_train'])
                else:
                    announced = datetime.strptime(str(df[i]['dateadded']), '%Y-%m-%d %H:%M:%S')
                    arrived = datetime.strptime(str(df[i + 1]['dateadded']), '%Y-%m-%d %H:%M:%S')
                    headway = (arrived - announced).total_seconds()
                    report = pd.DataFrame({'trip_num': int(df[i]['trip_num']),
                                           'time_announced': str(df[i]['dateadded']),
                                           'projected_duration': int(df[i]['Time1_Est']),
                                           'time_arrived': str(df[i+1]['dateadded']),
                                           'actual_duration': int(headway),
                                           'time_since_last_train': int(df[i+1]['time_since_last'])}, index=[0])
                    station_dict[df[i]['StationID']] = station_dict[df[i]['StationID']].append(report)
                if stats:
                    if 'general' not in stats_dict:
                        stats_dict['general'] = {'headways' : [], 'headway_deltas': []}
                    if df[i]['StationID'] not in stats_dict:
                        stats_dict[df[i]['StationID']] = {'headways' : [], 'headway_deltas' : []}
                    else:
                        if str(df[i+1]['time_since_last']) != 'None':
                            stats_dict[df[i]['StationID']]['headways'].append(int(df[i+1]['time_since_last']))
                            stats_dict['general']['headways'].append((int(df[i+1]['time_since_last'])))
                        if str(df[i + 1]['est_error']) != 'None':
                            stats_dict[df[i]['StationID']]['headway_deltas'].append(int(df[i + 1]['est_error']))
                            stats_dict['general']['headway_deltas'].append((int(df[i + 1]['est_error'])))
                i += 2
            else:
                i += 1
        except:
            i += 1
    #reorganize columns and sort by time_announced
    for k in list(station_dict):
        station_dict[k] = station_dict[k][['trip_num','time_announced','projected_duration','time_arrived','actual_duration','time_since_last_train']]
        station_dict[k] = station_dict[k].sort_values(by=['time_announced'])
    for k in list(stats_dict):
        stats_dict[k]['headways'] = np.array(stats_dict[k]['headways'])
        stats_dict[k]['headway_deltas'] = np.array(stats_dict[k]['headway_deltas'])
    return (stats_dict, station_dict)

def make_json_string(stats_dict, station_dict, stats, timespan_start, timespan_end):
    if stats:
        json_dict = \
            {"\"transit_report\"": {
                    "timespan_start" : timespan_start,
                    "timespan_end" : timespan_end,
                    "data" : {
                        "summary_statistics" : {},
                        "trips_by_station": {}
                    }
                }
            }
    else:
        json_dict = \
            {"transit_report": {
                "timespan_start": timespan_start,
                "timespan_end": timespan_end,
                "data": {
                    "trips_by_station": {}
                }
            }
            }
    #access and edit nested lists
    for k in list(json_dict):
        for k1 in list(json_dict[k]):
            if type(json_dict[k][k1]) == dict:
                for k2 in list(json_dict[k][k1]):
                    for s in list(station_dict):
                        if k2 == 'summary_statistics':
                            if 'GENERAL' not in json_dict[k][k1][k2]:
                                json_dict[k][k1][k2]['GENERAL'] = {'headways' : {'mean': str(np.mean(stats_dict['general']['headways'])),
                                                                    'std_dev' : str(np.std(stats_dict['general']['headways'])),
                                                                    'min' : str(np.amin(stats_dict['general']['headways'])),
                                                                    'max' : str(np.amax(stats_dict['general']['headways'])),
                                                                    'median' : str(np.percentile(stats_dict['general']['headways'], 50)),
                                                                    '25th_ptile' : str(np.percentile(stats_dict['general']['headways'], 25)),
                                                                    '75th_ptile' : str(np.percentile(stats_dict['general']['headways'], 75))},
                                                                   'headway_deltas' : {'mean': str(np.mean(stats_dict['general']['headway_deltas'])),
                                                                    'std_dev' : str(np.std(stats_dict['general']['headway_deltas'])),
                                                                    'min' : str(np.amin(stats_dict['general']['headway_deltas'])),
                                                                    'max' : str(np.amax(stats_dict['general']['headway_deltas'])),
                                                                    'median' : str(np.percentile(stats_dict['general']['headway_deltas'], 50)),
                                                                    '25th_ptile' : str(np.percentile(stats_dict['general']['headway_deltas'], 25)),
                                                                    '75th_ptile' : str(np.percentile(stats_dict['general']['headway_deltas'], 75))}}
                            json_dict[k][k1][k2][s] = {'headways': {'mean': str(np.mean(stats_dict[s]['headways'])),
                                                                    'std_dev' : str(np.std(stats_dict[s]['headways'])),
                                                                    'min' : str(np.amin(stats_dict[s]['headways'])),
                                                                    'max' : str(np.amax(stats_dict[s]['headways'])),
                                                                    'median' : str(np.percentile(stats_dict[s]['headways'], 50)),
                                                                    '25th_ptile' : str(np.percentile(stats_dict[s]['headways'], 25)),
                                                                    '75th_ptile' : str(np.percentile(stats_dict[s]['headways'], 75))
                                                                    },
                                                       'headway_deltas': {
                                                                        'mean': str(np.mean(stats_dict[s]['headway_deltas'])),
                                                                        'std_dev' : str(np.std(stats_dict[s]['headway_deltas'])),
                                                                        'min' : str(np.amin(stats_dict[s]['headway_deltas'])),
                                                                        'max' : str(np.amax(stats_dict[s]['headway_deltas'])),
                                                                        'median' : str(np.percentile(stats_dict[s]['headway_deltas'], 50)),
                                                                        '25th_ptile' : str(np.percentile(stats_dict[s]['headway_deltas'], 25)),
                                                                        '75th_ptile' : str(np.percentile(stats_dict[s]['headway_deltas'], 75))
                                                                    }
                                                       }
                        elif k2 == 'trips_by_station':
                            json_dict[k][k1][k2][s] = station_dict[s].to_dict(orient='records')

    json_str = json.dumps(json_dict, indent=3, separators=(',', ': '), sort_keys=True)
    print(json_str)

timespan_start = sys.argv[1]
timespan_end = sys.argv[2]
stats = bool(sys.argv[3])
station_list = sys.argv[4].split("%")
# timespan_start = "2017-12-22 12:00:00"
# timespan_end = "2017-12-22 23:00:00"
# stats = True
# station_list = ['GVT-NB', 'GVT-SB', "BLK-NB", "BLK-SB","CVC-NB", "CVC-SB","MLK-NB", "MLK-SB"]

#grab data
df = connect_and_return_data(timespan_start, timespan_end)
#once we know trainTrip is good, this won't have to be done (will make process about twice as fast)
df = clean_train_trip(df)
# #respond to get request
stats_dict, station_dict = make_data_dicts(df, stats, station_list)
make_json_string(stats_dict, station_dict, stats,timespan_start,timespan_end)


