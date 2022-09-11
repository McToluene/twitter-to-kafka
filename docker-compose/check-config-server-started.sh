#!/bin/bash
# check-config-server-started.sh

app-get updaye -y 
yes | apt-get install curl
curlResult=$(curl -s -o /dev/null - I -w "%{http_code}" http://config-server:8888/acuator/health)
echo "result status code:" $curlResult
while [[ ! $curlResult == "200"]]; do
    >&2 echo "Config server is not up yet!"
    sleep 2
    curlResult=$(curl -s -o /dev/null - I -w "%{http_code}" http://config-server:8888/acuator/health)
done

./cnb/lifecycle/launcher