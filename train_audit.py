import pandas as pd
from datetime import datetime
import sqlite3
import numpy as np
import const
import copy
import matplotlib.pyplot as plt
import json

#NOTE: apparently many trains are called "SCH"...these seem to be ghost trains. ids that are ints seem to be good, though.

#make filtered db
#1
def load_data(csv_path, db_path):

    query = """
    CREATE TABLE IF NOT EXISTS trips(
    station_id text,
    time_announced datetime,
    projected_duration int,
    time_arrived datetime,
    actual_duration int,
    time_since_last_train int,
    line_id text,
    train_id text,
    trip_id text,
    PRIMARY KEY(time_announced, station_id)
    );
    """
    #make table
    sql_query(db_path, query)

    #read db
    with open(csv_path) as csvfile:
        df = pd.read_csv(csvfile)
    #goes into messy db and cleans/organizes by station_id, with one report per row (either beginning or end of trip)
    cur_announced_and_arrived = munge_data(df)

    try:
        db = sqlite3.connect(db_path, isolation_level=None)
        query = """
        SELECT * FROM
        trips
        WHERE
        trip_id = (SELECT
        MAX(trip_id)
        FROM
        trips);
        """
        last_row_in_table = pd.read_sql_query(query, db)
        last_station_in_table = last_row_in_table.ix[0]['station_id']
        db.close()
    except:
        print("error selecting in load_data")

    trips = make_trips_df(cur_announced_and_arrived)

    query = 'INSERT OR IGNORE INTO trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
    sql_query(db_path, query, many=True, many_list=trips)

#2(included in 1)
def munge_data(messy_table):
    print('grouping trips...')
    trip_i1 = 0
    trips = []
    cur_announced_and_arrived = pd.DataFrame(columns=['datetime',
                                                      'station_id',
                                                      'projection',
                                                      'trip_id',
                                                      'train_id',
                                                      'line_id'])
    #go through every station
    for st in const.stations:
        trip_id = 1
        #go through all rows
        for i in range(messy_table.shape[0]):
            #first train
            if i == 0:
                train = messy_table.ix[i][st + '_Time1_Train']
                trip_i1 = i
            # new train
            if train != messy_table.ix[i][st + '_Time1_Train']:
                announced_report = ()
                arrived_report = ()
                excep = False
                #go through all rows in current trip
                for j in [trip_i1, i - 1]:
                    try:
                        est_secs = int(messy_table.ix[j][st + '_Time1_Est'])
                        m, s = divmod(est_secs, 60)
                        h, m = divmod(m, 60)
                        eta = str(h) + ":" + str(m) + ":" + str(s)

                        report = pd.dataFrame({'datetime' : datetime.strptime(messy_table.ix[j]['dateadded'], '%Y-%m-%d %H:%M:%S'),
                                               'station_id' : st,
                                               'projection' : eta,
                                               'trip_id' : trip_id,
                                               'train_id' : str(messy_table.ix[j][st + '_Time1_Train']),
                                               'line_id' : messy_table.ix[j][st + '_Time1_LineID']}, index=[0])
                        if j == trip_i1:
                            announced_report = report
                        elif j == i - 1:
                            arrived_report = report
                    except:
                        excep = True
                        print("bad time " + str(messy_table.ix[j][st + '_Time1_Est']))
                #insert only good trips
                if not excep and len(announced_report) == 6 and len(arrived_report) == 6:
                    x = announced_report.ix[0]['projection'].split(":")
                    an_proj = int(x[0]) + 60*int(x[1]) + int(x[2])
                    x = arrived_report.ix[0]['projection'].split(":")
                    arr = int(x[0]) + 60*int(x[1]) + int(x[2])
                    if an_proj > 0 and arr == 0 and an_proj > arr:
                        cur_announced_and_arrived = cur_announced_and_arrived.append(announced_report)
                        cur_announced_and_arrived = cur_announced_and_arrived.append(arrived_report)
                        trips.append(announced_report)
                        trips.append(arrived_report)
                        trip_id += 1
                    excep = False

                #go to next trip
                train = messy_table.ix[i][st + '_Time1_Train']
                trip_i1 = i

    print("done")
    return cur_announced_and_arrived

def make_trips_df(cur_announced_and_arrived):
    i = 0
    #go through all rows
    trips = pd.DataFrame(columns=['station_id',
                                  'time_announced',
                                  'projected_duration',
                                  'time_arrived',
                                  'actual_duration',
                                  'time_since_last_train',
                                  'line_id',
                                  'train_id',
                                  'trip_id'
                                  ])
    while i < cur_announced_and_arrived.shape[0] - 1:
        train_id = cur_announced_and_arrived.ix[i]['train_id']
        if train_id not in ['SCH', 'DLY']:
            announced = datetime.strptime(cur_announced_and_arrived.ix[i]['dateadded'], '%Y-%m-%d %H:%M:%S')
            arrived = datetime.strptime(cur_announced_and_arrived.ix[i + 1]['dateadded'], '%Y-%m-%d %H:%M:%S')
            headway = (arrived - announced).total_seconds()
            trip_report = pd.DataFrame({'station_id' : cur_announced_and_arrived.ix[i]['station_id'],
                                        'time_announced': cur_announced_and_arrived.ix[i]['dateadded'],
                                        'projected_duration': cur_announced_and_arrived.ix[i]['projection'],
                                        'time_arrived': cur_announced_and_arrived.ix[i + 1]['dateadded'],
                                        'actual_duration': headway / 60,
                                        'time_since_last_train': 0,
                                        'line_id' : cur_announced_and_arrived.ix[i]['line_id'],
                                        'train_id' : cur_announced_and_arrived.ix[i]['train_id'],
                                        'trip_id' : cur_announced_and_arrived.ix[i]['trip_id']
                                        }, index=[0])
            trip_report = trip_report[['station_id',
                                       'time_announced',
                                       'projected_duration',
                                       'time_arrived',
                                       'actual_duration',
                                       'time_since_last_train',
                                       'line_id',
                                       'train_id',
                                       'trip_id'
                                     ]]
            trips = trips.append(trip_report)
        i += 2

def sql_query(db_path, query, many=False, many_list=None):
    print(query + " ...")
    db = sqlite3.connect(db_path, isolation_level=None)
    conn = db.cursor()
    try:
        if many:
            conn.executemany(query, many_list)
        else:
            conn.execute(query)
    except:
        print("sql error for " + query)
    conn.close()
    db.close()
    print("done")



def make_json(db_path):

    try:
        db = sqlite3.connect(db_path, isolation_level=None)
        query = "SELECT * FROM trips"
        trips = pd.read_sql_query(query, db)
        db.close()
    except:
        print("error selecting in make_json")
    trips_dict = dict()
    i = 0
    while i < trips.shape[0] - 1:
        train_id = trips.ix[i]['train_id']
        if train_id not in ['SCH', 'DLY']:
            if train_id not in trips_dict:
                trips_dict[train_id] = []
            else:
                announced = datetime.strptime(trips.ix[i]['dateadded'], '%Y-%m-%d %H:%M:%S')
                arrived = datetime.strptime(trips.ix[i + 1]['dateadded'], '%Y-%m-%d %H:%M:%S')
                headway = (arrived - announced).total_seconds()
                trip_report = pd.DataFrame({'time_announced' : trips.ix[i]['dateadded'],
                                            'projected_duration' : trips.ix[i]['projection'],
                                            'time_arrived' : trips.ix[i+1]['dateadded'],
                                            'actual_duration' : headway/60,
                                            'time_since_last_train' : 0
                                            }, index=[0])
                trip_report = trip_report[['time_announced', 'projected_duration', 'time_arrived', 'actual_duration', 'time_since_last_train']]
                trips_dict[train_id].append(trip_report)

        i += 2
    for k, v in trips_dict.items():
        for i in range(len(v)):
            if i == 0:
                v[i].loc[0, 'time_since_last_train'] = 0
            else:
                last_arrival_time = datetime.strptime(v[i-1].ix[0]['time_arrived'], '%Y-%m-%d %H:%M:%S')
                cur_arrival_time = datetime.strptime(v[i].ix[0]['time_arrived'], '%Y-%m-%d %H:%M:%S')
                headway = (last_arrival_time - cur_arrival_time).total_seconds()
                v[i].loc[0, 'time_since_last_train'] = headway / 60
    for k, v in trips_dict.items():
        print(k + ":\n " + str(v) )

    json_str = json.dumps(
    {"trip_reports" : {
        "timespan_report" : {
            "timespan_start" : "YYYY-MM-DD HH:MM:SS",
            "timespan_end" : "YYYY-MM-DD HH:MM:SS",
                "station_report" : {
                    "station_id" : "x",
                    "trips": [{"time_announced" : "HH:MM:SS",
                               "projected_duration" : "x",
                               "time_arrived" : "HH:MM:SS",
                               "actual_duration" : "x",
                               "time_since_last_train": "x"},
                    ]
                }
            }
        }
    }, indent = 3, separators=(',', ': '))

    print(json_str)

#samples for statistics computation
def collect_samples(db_path, interval):
    try:
        db = sqlite3.connect(db_path, isolation_level=None)
        query = "SELECT * FROM trips"
        trips = pd.read_sql_query(query, db)
        db.close()
    except:
        print("error selecting in collect_samples")

    #all trips are good
    i = 0
    hdw = []
    hdw_deltas = []
    hdw_map = dict()
    hdw_deltas_map = dict()
    while i < trips.shape[0] - 1:
        ghost_train = False
        try:
            int(trips.ix[i]['train_id'])
        except:
            ghost_train = True

        #good station
        if trips.ix[i]['trip_id'] == trips.ix[i + 1]['trip_id'] and not ghost_train:
            cur_station = trips.ix[i]['station_id']
            announced = datetime.strptime(trips.ix[i]['dateadded'], '%Y-%m-%d %H:%M:%S')
            arrived = datetime.strptime(trips.ix[i + 1]['dateadded'], '%Y-%m-%d %H:%M:%S')
            headway = (arrived - announced).total_seconds()
            x = trips.ix[i]['projection'].split(":")
            projected_headway = int(x[0]) + 60 * int(x[1]) + int(x[2])

            if announced >= interval[0] and announced <= interval[1]:
                hdw.append(headway / 60)
                hdw_deltas.append((headway - projected_headway) / 60)
                # if(headway / 60) > 50:
                #     print(trips.ix[i]['train_id'])
                #     print(trips.ix[i+1]['train_id'])
                #     print(announced)
                #     print(arrived)
                #     print(headway / 60)
                #     print(cur_station)
                #     print(trips.ix[i]['trip_id'])
                #     print(trips.ix[i+1]['trip_id'])
            if i + 2 < trips.shape[0] - 1:
                if cur_station == trips.ix[i + 2]['station_id']:
                    i += 2
                else:
                    hdw_map[cur_station] = copy.deepcopy(hdw)
                    hdw_deltas_map[cur_station] = copy.deepcopy(hdw_deltas)
                    hdw.clear()
                    hdw_deltas.clear()
                    i += 2
            else:
                hdw_map[cur_station] = copy.deepcopy(hdw)
                hdw_deltas_map[cur_station] = copy.deepcopy(hdw_deltas)
                hdw.clear()
                hdw_deltas.clear()
                i += 2
        #check next pair
        else:
            i+=1

    return [("Headways: ", hdw_map), ("Headway Deltas: ", hdw_deltas_map)]

def compute_stats(sample_maps):
    for map_tuple in sample_maps:
        for k, v in map_tuple[1].items():
            stats = ""
            if len(v) > 0:
                np_array = np.asarray(v, dtype=float)
                plt.hist(np_array, bins='auto')
                plt.savefig('plots/' + k + '_' + map_tuple[0][:-2] + '.png')
                plt.close()
                mean = np_array.mean()
                st_dev = np_array.std()
                median = np.percentile(np_array, 50)
                q1 = np.percentile(np_array, 25)
                q3 = np.percentile(np_array, 75)
                iqr = q3 - q1
                min = np_array.min()
                max = np_array.max()
                range = max - min
                stats = "mean: " + str(mean) + "\nst_dev: " + str(st_dev) + "\nmedian: " + str(median) \
                        + "\nq1: " + str(q1) + "\nq3: " + str(q3) + "\niqr: " + str(iqr) + "\nmin: " + str(min) \
                        + "\nmax: " + str(max) + "\nrange: " + str(range)
            result = k + "\n"
            result += "num_trips: " + str(len(v)) + "\n"
            result += map_tuple[0] + "\n"
            result += stats
            print(result + "\n")

###############
#main
###############
db_path = 'trips.db'
load = True
if load:
    load_data('train_small.csv', db_path)

#if date in interval...
# interval = [datetime.strptime('2017-11-04 16:21:00','%Y-%m-%d %H:%M:%S'),
#             datetime.strptime('2017-11-05 20:00:00', '%Y-%m-%d %H:%M:%S')]
#
# #stats computation
# #sample_maps = collect_samples(db_path, interval)
# # compute_stats(sample_maps)
# #json request
# make_json(db_path)

