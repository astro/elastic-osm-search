// TODO: https://github.com/ncolomer/elasticsearch-osmosis-plugin/wiki/Data-mapping

var expat = require('node-expat');
var request = require('request');


function bulkReq(batch, cb) {
    request({
        method: 'POST',
        url: "http://localhost:9200/osm/_bulk",
        body: batch,
        json: true
    }, function(err, res, body) {
        if (err) {
            cb(err);
        } else if (res.statusCode != 200) {
            cb(new Error(body && body.error || "Unknown error"));
        } else {
            cb(null, body);
        }
    });
}

var CONCURRENCY = 4;
var uploadsPending = 0;
function doUpload(batch) {
    bulkReq(batch, function(err) {
        // console.log("bulkReq", arguments);
        if (err) {
            console.error(err.stack || err);
        }
        uploadsPending--;
        if (uploadsPending < CONCURRENCY) {
            // console.log("resume,", uploadsPending, "pending");
            process.stdin.resume();
        } // else
            // console.log("not resuming,", uploadsPending, "pending");
    });
    uploadsPending++;
    if (uploadsPending >= CONCURRENCY) {
        // console.log("pause,", uploadsPending, "pending");
        process.stdin.pause();
    }
}

var BATCH_SIZE = 1024;
var batch = [];
function onElement(type, body) {
    if (body.lat && body.lon) {
        body.lat_lon = body.lat + "," + body.lon;
        body.lon_lat = [body.lon, body.lat];
    }
    ['id', 'version', 'changeset', 'ele', 'height', 'floors', 'circumference'].forEach(function(numberField) {
        if (body[numberField]) {
            body[numberField] = Number(body[numberField]);
        }
    });
    var bulkCmd = {
        index: {
            _type: type,
            _id: body.id
        }
    };
    batch.push(JSON.stringify(bulkCmd) + "\n" + JSON.stringify(body) + "\n");

    if (batch.length >= BATCH_SIZE) {
        flushBatch();
    }
}
function flushBatch() {
    doUpload(batch.join(""));
    batch = [];
}

var parser = new expat.Parser();
var state, current;
parser.on('startElement', function(name, attrs) {
    if (!state && 
        (name == 'node' ||
         name == 'way' ||
         name == 'relation')) {
        current = attrs;
        state = name;
    } else if (state && name == 'tag' && !current.hasOwnProperty(attrs.k)) {
        current[attrs.k] = attrs.v;
    } else if (state && name == 'nd') {
        if (!current.nd) {
            current.nd = [];
        }
        current.nd.push(attrs.ref);
    } else if (state && name == 'member') {
        if (!current.members) {
            current.members = [];
        }
        current.members.push(attrs);
    } else {
        console.log('in', state, 'unhandled startElement', name, attrs);
    }
});
parser.on('endElement', function(name) {
    if (state && name == state) {
        onElement(state, current);

        state = null;
        current = null;
    }
});

process.stdin.resume();
process.stdin.pipe(parser);

parser.on('end', flushBatch);
parser.on('end', function() {
    console.log("endDocument");
});
process.stdin.on('end', function() {
    if (batch.length > 0) {
        console.warn(batch.length + " not processed, flushing again.");
        flushBatch();
    }
    console.log("Fin.");
});
