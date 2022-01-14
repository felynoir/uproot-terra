import sys
import json
import base64
import pandas as pd

from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from proto import query_pb2

x = query_pb2.QueryContractStoreRequest()

# TODO: change params below
x.contract_address = "terra1xacqx447msqp46qmv8k2sq6v5jh9fdj37az898"
msg = {"state": {}}
start_block = 6070000
end_block = 6070100
api_key = ""
x.query_msg = json.dumps(msg).encode("utf-8")

session = FuturesSession(max_workers=5)
retries = 5
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
datas = []
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

    # TODO: just do a workaround
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
    datas.append(decoded_val)

    # progress stuff
    done += 1
    sys.stdout.write("\r[%s%s] " % ("=" * done, " " * (end_block - done - start_block)))
    sys.stdout.flush()

pd.DataFrame(datas).sort_values("height").to_csv(
    open("csv/lunax_states.csv", "w"), index=False
)
print("\n--- %s seconds ---" % (time.time() - start_time))
