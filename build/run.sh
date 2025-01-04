
# docker pull elasticsearch
# https://hub.docker.com/_/elasticsearch

#  https://dev.to/ussdlover/setting-up-elasticsearch-with-docker-a-quick-guide-2fll

# docker pull elasticsearch:8.10.2
# docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:8.10.2

docker pull elasticsearch:8.10.2

docker run -d --name elasticsearch \
    -p 9200:9200 \
    -p 9300:9300 \
    -e "discovery.type=single-node" \
    -e "xpack.security.enabled=false" \
    -e "xpack.security.transport.ssl.enabled=false" \
    -e "xpack.security.http.ssl.enabled=false" \
    -e "network.host=0.0.0.0" \
    elasticsearch:8.10.2

while true; do
    sleep 1
done

