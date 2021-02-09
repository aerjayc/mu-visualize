import mangaupdates
import pandas as pd
import requests
import json
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

if __name__ == '__main__':
    csvfname = 'top1k.csv'
    jsonfname = 'top.json'
    save_progress_period = 10

    data = pd.read_csv(csvfname)
    with open(jsonfname) as f:
        saved = json.loads(f.read())
    print("Initial saved data has", len(saved), "entries.")

    series_ids = data['seriesid'].unique()

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
                if i % save_progress_period == save_progress_period - 1:
                    print("\nSaving progress...", end= ' ', flush=True)
                    with open(jsonfname, 'w') as f:
                        f.write(json.dumps(saved, indent=4))
                    print("Done.")

                time.sleep(30)
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
