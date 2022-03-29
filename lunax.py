import sys
import json
import base64
from tracemalloc import start
import pandas as pd
import os

from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from proto import query_pb2

x = query_pb2.QueryContractStoreRequest()

# NOTE: env setup
api_key = os.getenv("DATAHUB_API_KEY")

# TODO: change params below
file_path = "csv/states.csv"
max_workers = 100
retries = 5
x.contract_address = "terra1xacqx447msqp46qmv8k2sq6v5jh9fdj37az898" # contract addr
msg = {"state": {}} # query msg
start_block = 6070000 # start block archival
end_block = 6090000 # # end block archival

# CLI
# python <this_file> [-r]
# -r to reset file
if '-r' in sys.argv:
    if os.path.exists(file_path):
        os.remove(file_path)
        print('remove file success.')
    else:
        print('no file exits.')


datas = pd.DataFrame()
# if file exist continue from last time
if os.path.exists(file_path):
    datas = pd.read_csv(file_path, index_col=False)
    start_block = datas['height'].max() + 1 
    # print([tuple(x) for x in datas.values])

def save_file():
    pd.DataFrame(datas).sort_values('height').to_csv(
            open(file_path, "w"),  index=False
    )

# setup
x.query_msg = json.dumps(msg).encode("utf-8")
session = FuturesSession(max_workers=max_workers)
status_forcelist = [429]
retry = Retry(
    total=retries,
    read=retries,
    connect=retries,
    respect_retry_after_header=True,
    status_forcelist=status_forcelist,
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

import time
start_time = time.time()
done = 0

futures = []
for i in range(start_block, end_block + 1):
    future = session.get(
        f"https://columbus-5--rpc--archive.datahub.figment.io/apikey/{api_key}/abci_query?path=%22/terra.wasm.v1beta1.Query/ContractStore%22&data=0x{x.SerializeToString().hex()}&height={str(i)}"
    )
    future.height = i
    futures.append(future)

for future in as_completed(futures):
    try:
        res = future.result()
    except Exception as e:
        print(e)
        continue
    res = res.json()
    value = res["result"]["response"]["value"]

    # TODO: workaound remove special character
    idx = 0
    while idx < len(value):
        try:
            decoded_val = base64.b64decode(value[idx:]).decode()
            break
        except:
            idx += 1

    # TODO: change by response type
    decoded_val = json.loads(decoded_val)["state"]
    decoded_val["height"] = future.height

    datas = datas.append(decoded_val, ignore_index=True)
    # workaround pd auto convert to float
    datas['height'] = datas['height'].astype(int)

    # progress stuff
    done += 1
    percent = done / (end_block-start_block) * 100
    sys.stdout.write("\r[%s%s] " % ("=" * int(percent), " " * int(100-percent)))
    sys.stdout.flush()
    if done%1000 == 0:
        save_file()


save_file()

print("\n--- %s seconds ---" % (time.time() - start_time))

