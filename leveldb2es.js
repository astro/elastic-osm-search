var async = require('async');
var leveldown = require('leveldown-hyper');
var db = leveldown('./osm');
var elasticsearch = require('elasticsearch');
var es = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});
var Readable = require('stream').Readable;
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');

var INTERESTING = [
    "amenity", "emergency", "historic",
    "leisure", "public_transport", "shop",
    "sport", "tourism", "craft",
    "office", "addr:housenumber"
];

util.inherits(DBStream, Readable);
function DBStream() {
    Readable.call(this, { objectMode: true, highWaterMark: 0 });
    this.iter = db.iterator({
        keyAsBuffer: false,
        valueAsBuffer: true
    });
    this.count = 0;
}

DBStream.prototype._read = function() {
    this.iter.next(function(err, key, value) {
        if (err) {
            this.emit('error', err);
            return;
        }

        if (key && value) {
            this.count++;
            value = JSON.parse(value);

            var isInteresting = INTERESTING.some(function(field) {
                return value.tags.hasOwnProperty(field);
            });
            if (isInteresting) {
                this.push({ key: key, value: value });
            } else {
                this._read();
            }
        } else {
            this.push(null);
            this.iter.end();
        }
    }.bind(this));
};


util.inherits(DBResolver, Transform);
function DBResolver() {
    Transform.call(this, { objectMode: true, highWaterMark: 0 });
}

DBResolver.prototype._transform = function(pair, encoding, callback) {
    var done = function(err) {
        if (err) {
            callback(err);
        } else {
            this.push(pair.value);
            callback();
        }
    }.bind(this);

    var value = pair.value;
    switch(value.type) {
    case 'node':
        value.center = [value.lon, value.lat];
        done();
        break;
    case 'way':
        this.expandWay(value, done);
        break;
    case 'relation':
        this.expandRelation(value, done);
        break;
    }
};


function getEl(id, cb) {
    db.get(id, {
        asBuffer: true
    }, function(err, s) {
        if (err) {
            cb(err);
        } else {
            var value = JSON.parse(s);
            cb(null, value);
        }
    });
}

function getNodeLatLon(id, cb) {
    getEl(id, function(err, value) {
        if (err) {
            cb(err);
        } else {
            cb(null, [value.lat, value.lon]);
        }
    });
}

DBResolver.prototype.expandWay = function(way, callback) {
    async.map(way.refs, function(ref, cb) {
        getNodeLatLon(ref, function(err, latlon) {
            if (err)
                console.error(err);
            cb(null, latlon);
        });
    }, function(err, refCoordinates) {
        // console.log("expanded", way, "to", err, refCoordinates);
        // console.log("expanded", way.id, "to", err, refCoordinates.length);
        if (err) {
            console.log("err", err);
            /* Don't set location */
            callback(err);
            return;
        }

        refCoordinates = refCoordinates.filter(function(coords) {
            return coords && coords[0] && coords[1];
        });

        var closed = way.refs[0] === way.refs[way.refs.length - 1];
        way.location = {
            type: closed ? 'polygon' : 'linestring',
            coordinates: closed ? [refCoordinates] : refCoordinates
        };
        var lat = 0, lon = 0;
        for(var i = 0; i < refCoordinates.length; i++) {
            lat += refCoordinates[i][0];
            lon += refCoordinates[i][1];
        }
        lat /= refCoordinates.length;
        lon /= refCoordinates.length;
        way.center = [lon, lat];
        callback();
    });
};

DBResolver.prototype.expandRelation = function(rel, callback) {
    async.mapSeries(rel.members, function(ref, cb) {
        if (ref.type === 'way') {
            getEl(ref.id, function(err, way) {
                if (err) {
                    cb(err);
                } else {
                    this.expandWay(way, function(err) {
                        var loc = way.location;
                        cb(err, loc && {
                            inner: ref.role === 'inner' || ref.role === 'enclave',
                            coordinates: loc.type == 'polygon' ? loc.coordinates : [loc.coordinates]
                        });
                    });
                }
            }.bind(this));
        } else {
            cb();
        }
    }.bind(this), function(err, refCoordinates) {
        if (err) {
            console.log("err", err);
            /* Don't set location */
            callback();
            return;
        }
        refCoordinates = refCoordinates.filter(function(c) { return !!c; });

        if (rel.tags.type === 'multipolygon' || rel.tags.type === 'boundary') {
            var outers = [];
            var inners = [];
            refCoordinates.forEach(function(r) {
                if (r.inner) {
                    inners.push(r.coordinates);
                } else {
                    outers.push(r.coordinates);
                }
            });

            if (outers.length < 2) {
                rel.location = {
                    type: 'polygon',
                    coordinates: outers.concat(inners)
                };
            } else {
                rel.location = {
                    type: 'multipolygon',
                    coordinates: outers.map(function(outer) {
                        return [outer].concat(inners);
                    })
                };
            }
        } else {
            rel.location = {
                type: 'multilinestring',
                coordinates: refCoordinates.map(function(c) {
                    return c && c.coordinates;
                })
            };
        }
        callback();
    });
};


util.inherits(BatchBuffer, Transform);
function BatchBuffer(batchSize) {
    Transform.call(this, { objectMode: true });
    this._readableState.highWaterMark = 1;
    this._writableState.highWaterMark = batchSize;
    this.batchSize = batchSize;
    this.buffer = [];
}

BatchBuffer.prototype._transform = function(chunk, encoding, callback) {
    this.buffer.push(chunk);
    this.canFlush(false);
    callback();
};

BatchBuffer.prototype._flush = function(callback) {
    this.canFlush(true);
    callback();
};

BatchBuffer.prototype.canFlush = function(force) {
    while((force && this.buffer.length > 0) || this.buffer.length >= this.batchSize) {
        var chunks = this.buffer.slice(0, this.batchSize);
        this.buffer = this.buffer.slice(this.batchSize);
        this.push(chunks);
    }
};


util.inherits(ToES, Writable);
function ToES() {
    Writable.call(this, { objectMode: true, highWaterMark: 0 });
    this.count = 0;
}

ToES.prototype._write = function(batch, encoding, callback) {
    // es.index({
    //     index: 'osm',
    //     type: pair.value.type,
    //     id: pair.key,
    //     body: pair.value,
    //     consistency: 'one'
    // }, function(err) {
    //     if (err) {
    //         console.log("index", pair.key, pair.value.location);
    //         console.error(err);
    //     }

    //     callback();
    // });
    if (batch.length < 1)
        return callback();

    body = [];
    batch.forEach(function(doc) {
        body.push({
            index: {
                _index: 'osm',
                _type: doc.type,
                _id: doc.id
            }
        });
        body.push(doc);
    });
    es.bulk({
        body: body,
        requestTimeout: 120000
    }, callback);
    this.count += batch.length;
};


db.open(function(err) {
    if (err) {
        console.log("open", err);
        process.exit(1);
    }

    var input = new DBStream();
    var trans = new DBResolver();
    var out = new ToES();
    input
        .pipe(trans)
        .pipe(new BatchBuffer(128))
        .pipe(out)
        .on('end', function() {
            db.close();
        });

    setInterval(function() {
        console.log(input.count + " in, " + out.count + " out");
    }, 1000);
});
