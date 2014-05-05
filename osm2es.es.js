// TODO: https://github.com/ncolomer/elasticsearch-osmosis-plugin/wiki/Data-mapping

var expat = require('node-expat');
var elasticsearch = require('elasticsearch');
var es = new elasticsearch.Client({
    host: '172.22.99.24:9200',
    log: 'warning'
});

function upload(type, body, cb) {
    es.index({
        index: 'osm',
        type: type,
        id: body.id,
        body: body
    }, cb);
}

var parser = new expat.Parser();
var state, current, uploadsPending = 0;
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
    } else {
        console.log('startElement', arguments);
    }
});
parser.on('endElement', function(name) {
    if (state && name == state) {
        var type = state, body = current;
        function go() {
            upload(type, body, function(err) {
                uploadsPending--;
                if (err) {
                    console.error(err.stack || err);
                    process.nextTick(go);
                    return;
                }
                process.stdin.resume();
            });
            uploadsPending++;
            if (uploadsPending > 4) {
                process.stdin.pause();
            }
        }
        go();

        state = null;
        current = null;
    }
});
process.stdin.resume();
process.stdin.pipe(parser);
