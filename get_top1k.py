import mangaupdates
import pandas as pd
import requests
import json
import time
import argparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(metavar='INPUT', dest='input',
                        help='csv file containing series ids')
    parser.add_argument(metavar='OUTPUT', dest='output',
                        help='json file where the output will be saved')
    parser.add_argument('-d', '--delay', default=10,
                        help='# of seconds of delay between GET requests')
    parser.add_argument('--save-every', dest='save_every', default=10,
                        help='# requests to do before saving progress')
    parser.add_argument('-f', '--force', action='store_true',
                        help='ignore progress (initial data from INPUT)')
    args = parser.parse_args()

    csvfname = args.input                   # 'top1k.csv'
    jsonfname = args.output                 # 'top.json'
    save_progress_period = args.save_every  # 10

    print('csvfname =', csvfname)
    print('jsonfname =', jsonfname)
    data = pd.read_csv(csvfname)
    series_ids = data['seriesid'].unique()

    if args.force:
        saved = {}
    else:   # resume
        with open(jsonfname) as f:
            saved = json.loads(f.read())
        print("Initial saved data has", len(saved), "entries.")

    sess = requests.Session()
    retries = Retry(total=5, backoff_factor=3)
    sess.mount('http://', HTTPAdapter(max_retries=retries))
    try:
        for i, sid in enumerate(series_ids):
            loaded = False
            if str(sid) not in saved:
                print(sid, end=' ', flush=True)

                series = mangaupdates.Series(int(sid), session=sess)  # need to cast to int b/c pandas uses int64 w/c is not JSON-serializable
                series.populate()

                saved[str(sid)] = json.loads(series.json())
                loaded = True

                # save progress every save_progress_period
                if i % save_progress_period >= save_progress_period - 1:
                    print("\nSaving progress...", end= ' ', flush=True)
                    with open(jsonfname, 'w') as f:
                        f.write(json.dumps(saved, indent=4))
                    print("Done.")

                time.sleep(int(args.delay))
    except (KeyboardInterrupt, requests.exceptions.ConnectionError) as e:
        print('\n', e, sep='')
        if loaded:
            print("Stopped after loading", sid)
        else:
            print("Stopped before loading", sid)

    print("Saving to", jsonfname)
    with open(jsonfname, 'w') as f:
        f.write(json.dumps(saved, indent=4))
    print("Done.")
