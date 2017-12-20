import pandas as pd
from datetime import datetime
import sqlite3
import numpy as np
import const
import copy
import matplotlib.pyplot as plt

#NOTE: apparently many trains are called "SCH"...these seem to be ghost trains. ids that are ints seem to be good, though.

def munge_data(df):
    print('grouping trips...')
    trip_i1 = 0
    trips = []
    for st in const.stations:
        t_id = 1
        for i in range(df.shape[0]):
            if i == 0:
                train = df.ix[i][st + '_Time1_Train']
                trip_i1 = i
            # new train
            if train != df.ix[i][st + '_Time1_Train']:
                an_report = ()
                arr_report = ()
                excep = False
                for j in [trip_i1, i - 1]:
                    try:
                        est_secs = int(df.ix[j][st + '_Time1_Est'])
                        m, s = divmod(est_secs, 60)
                        h, m = divmod(m, 60)
                        eta = str(h) + ":" + str(m) + ":" + str(s)

                        report = (datetime.strptime(df.ix[j]['dateadded'], '%Y-%m-%d %H:%M:%S'),
                                  st,
                                  eta,
                                  t_id,
                                  str(df.ix[j][st + '_Time1_Train']),
                                  df.ix[j][st + '_Time1_LineID'])
                        if j == trip_i1:
                            an_report = report
                        elif j == i - 1:
                            arr_report = report
                    except:
                        excep = True
                        print("bad time " + str(df.ix[j][st + '_Time1_Est']))
                #filter bad trips
                if not excep and len(an_report) == 6 and len(arr_report) == 6:
                    x = an_report[2].split(":")
                    an_proj = int(x[0]) + 60*int(x[1]) + int(x[2])
                    x = arr_report[2].split(":")
                    arr = int(x[0]) + 60*int(x[1]) + int(x[2])
                    if an_proj > 0 and arr == 0 and an_proj > arr:
                        trips.append(an_report)
                        trips.append(arr_report)
                        t_id += 1
                    excep = False

                train = df.ix[i][st + '_Time1_Train']
                trip_i1 = i
    print("done")
    return trips

def do_sql(db_path, query, many=False, many_list=None):
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

def load_data(csv_path, db_path):
    with open(csv_path) as csvfile:
        df = pd.read_csv(csvfile)

    trips = munge_data(df)

    query = 'DROP TABLE IF EXISTS trips'
    do_sql(db_path, query)

    query = """
    CREATE TABLE trips(
    dateadded datetime,
    station_id text,
    projection int,
    trip_id text,
    train_id text,
    line_id text,
    PRIMARY KEY(dateadded, station_id)
    );
    """
    do_sql(db_path, query)

    query = 'INSERT OR IGNORE INTO trips VALUES (?, ?, ?, ?, ?, ?)'
    do_sql(db_path, query, True, trips)

def collect_samples(db_path, interval):
    try:
        db = sqlite3.connect(db_path, isolation_level=None)
        query = "SELECT * FROM trips"
        trips = pd.read_sql_query(query, db)
        db.close()
    except:
        print("error selecting in compute_stats")

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
load = False
if load:
    load_data('train.csv', db_path)

#if date in interval...
interval = [datetime.strptime('2017-11-04 16:21:00','%Y-%m-%d %H:%M:%S'),
            datetime.strptime('2017-11-05 20:00:00', '%Y-%m-%d %H:%M:%S')]

sample_maps = collect_samples(db_path, interval)
compute_stats(sample_maps)

