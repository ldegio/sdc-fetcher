import sys
import time
import pandas as pd
from sdcclient import SdcClient

#
# Data fetch parameters
#
PAGE_SIZE = 1000
FETCH_LIMIT = 2000

ONE_HOUR_IN_S = 3600
TWO_HOURS_IN_S = 3600 * 4
ONE_DAY_IN_S = 3600 * 24
ONE_WEEK_IN_S = 3600 * 24 * 7
TWO_WEEKS_IN_S = 3600 * 24 * 14
FOUR_WEEKS_IN_S = 3600 * 24 * 28 * 4

#
# Details about the time ranges that the user can specify
# NOTE: the 'chunks' parameter allows to split big queries into
# multiple time chuncks, which is useful to avoid being punished 
# by the backend if the query is too big.
#
TIME_RANGES = {
    '4w' : {
        'window': FOUR_WEEKS_IN_S,
        'step': ONE_DAY_IN_S,
    },
    '2w' : {
        'window': TWO_WEEKS_IN_S,
        'step': ONE_HOUR_IN_S,
    },
    '1w' : {
        'window': ONE_WEEK_IN_S,
        'step': ONE_HOUR_IN_S,
    },
    '1d' : {
        'window': ONE_DAY_IN_S,
        'step': 600,
    },
    '1h' : {
        'window': ONE_HOUR_IN_S,
        'step': 10,
    }    
}

class Fetcher(object):
    def __init__(self, token):
        #
        # Connect to the backend
        #
        self.sdclient = SdcClient(token)

    def fetch(self, info, query, paging, start_ts, end_ts, nchunks):
        res = {'start': 0, 'end': 0, 'data': []}

        time_range = info['time_range']
        source_type = info['source_type']
        if 'filter' in info:
            filter = info['filter']
        else:
            filter = ''

        try:
            start = start_ts
            chunk_len = TIME_RANGES[time_range]['window'] / nchunks
            end = start + chunk_len
            delta = TIME_RANGES[time_range]['step']
        except:
            raise Exception('fetch', 'unsupported time window %s.' % (str(time_range)))

        #
        # get the data
        #
        while start < end_ts:
            sys.stdout.write('.')
            sys.stdout.flush()

            gdres = self.sdclient.get_data(query,
                                        start,
                                        end,
                                        delta,
                                        filter,
                                        source_type,
                                        paging)

            if gdres[0] is False:
                if gdres[1].find('code 504') != -1:
                    print 'got a 504 from server.'
                    return None
                elif gdres[1].find('something really bad happened with your reques') != -1:
                    return None
                raise Exception('get_data', gdres[1])
            if res['start'] == 0:
                res['start'] = gdres[1]['start']
            res['end'] = gdres[1]['end']
            res['data'].append(gdres[1]['data'])

            start += chunk_len
            end += chunk_len

        res['query'] = query
        res['delta'] = delta
        return res

    def fetch_as_datatable(self, info, query):
        page_size = PAGE_SIZE
        fetch_limit = FETCH_LIMIT
        cur = 0
        dl_size = 0
        self.start_ts = 0
        self.end_ts = 0

        #
        # Determine the exact time interval to fetch
        #
        time_range = info['time_range']

        if not time_range in TIME_RANGES:
            raise Exception('fetch', 'unsupported time window %s.' % (str(time_range)))
            
        sampling = TIME_RANGES[time_range]['step'] * 1000000
        rires = self.sdclient.get_data_retention_info()
        if rires[0] == False:
            raise Exception('get_data_retention_info', rires[1])
        ri = rires[1]

        fa = False
        for tl in ri['agents']:
            if tl['sampling'] == sampling or (tl['sampling'] == 1000000 and sampling == 10000000):
                self.end_ts = tl['to'] / 1000000
                self.start_ts = self.end_ts - TIME_RANGES[time_range]['window']
                fa = True
                break

        if fa == False:
            raise Exception('fetch_as_datatable', 'sampling %u not supported by the backend' % sampling)

        #
        # Fetch the data, subdividing it in pages of page_size entries
        #
        while cur < fetch_limit:
            nchunks = 1
            paging = {'from': cur, 'to': cur + page_size}

            while nchunks <= 64:
                data = self.fetch(info, query, paging, self.start_ts, self.end_ts, nchunks)

                if data == None:
                    nchunks = nchunks * 4 if nchunks < 4 else nchunks * 2
                    print 'request too big, trying to split into %d chuncks' % nchunks
                    time.sleep(3)
                else:
                    break

            if data == None:
                raise Exception('request still failing with %d chunks, skipping' % nchunks)

            if len(data['data']) == 0 or len(data['data'][0]) == 0:
                if 'df' in locals():
                    return df
                else:
                    return None

            #
            # Create the pandas table using the information in the dataset
            #
            cols = []
            template_row = {}
            for ci in data['query']:
                cols.append(ci['id'])
                template_row[ci['id']] = 0

            #
            # Fill the table
            #
            rows = []
            dl_size = 0

            for chunk in data['data']:
                dl_size += sys.getsizeof(chunk)  
                for r in chunk:
                    newrow = dict(template_row)
                    newrow['t'] = r['t']
                    j = 0
                    for c in cols:
                        newrow[c] = r['d'][j]
                        j = j + 1
                    rows.append(newrow)

            if cur == 0:
                df = pd.DataFrame(rows)
            else:
                df = df.append(rows)

            cur += (page_size + 1)
            print 'records: %d, bytes: %d' % (cur - 1, dl_size)

        return df
