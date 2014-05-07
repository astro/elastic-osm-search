#!/bin/sh

HOST=localhost:9200
[ "x$1" = "x" ] || HOST=$1

# Attention: drops index!
#curl -XDELETE "http://$HOST/osm"

curl -XPOST "http://localhost:9200/osm"

curl -XPUT "http://$HOST/osm/node/_mapping" -d '
{
    "node" : {
        "_id": { "path": "id" },
        "_all" : {"enabled" : true},
        "properties" : {
            "lat_lon" : {"type" : "geo_point" }
        }
    }
}'
