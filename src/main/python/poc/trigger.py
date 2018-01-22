import requests
import json
import sys
import datetime
import time

if __name__ == "__main__":
    url = sys.argv[1]
    data = {"entitiesAmount": sys.argv[2],
            "minLongitude": sys.argv[3],
            "maxLongitude": sys.argv[4],
            "minLatitude": sys.argv[5],
            "maxLatitude": sys.argv[6]}
    headers = {'Content-type': 'application/json'}

    while True:
        time.sleep(1)
        data["triggerTime"] = datetime.datetime.utcnow().isoformat()
        data_json = json.dumps(data)
        response = requests.post(url, data=data_json, headers=headers)
        print("response: " + str(response))
