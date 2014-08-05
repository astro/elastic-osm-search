var parseOSM = require('osm-pbf-parser');
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');
var async = require('async');
var elasticsearch = require('elasticsearch');
var es = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});


util.inherits(Expander, Transform);
function Expander() {
    Transform.call(this, { objectMode: true, highWaterMark: 4 });
}

Expander.prototype._transform = function(values, encoding, callback) {
    async.eachLimit(values, 4, function(value, cb) {
        if (value.type === 'node') {
            // return setImmediate(cb);
            value.location = {
                type: 'point',
                coordinates: [value.lon, value.lat]
            };
            setImmediate(cb);
        } else if (value.type === 'way') {
            // return setImmediate(cb);
            this.expandWay(value, function(err) {
                cb(err);
            }.bind(this));
        } else  if (value.type === 'relation') {
            this.expandRelation(value, function(err) {
                cb(err);
            }.bind(this));
        } else {
            console.warn("Unknown element", value.type, ":", value);
        }
    }.bind(this), function(err) {
        values = values.filter(function(v) {
            return v.type !== 'node';
        });
        var CONCURRENCY = 128;
        for(var i = 0; i < values.length; i += CONCURRENCY) {
            this.push(values.slice(i, i + CONCURRENCY));
        }
        // this.push(values);
        callback(err);
    }.bind(this));
};

Expander.prototype.expandWay = function(way, callback) {
    async.mapSeries(way.refs, function(ref, cb) {
        es.get({
            index: 'osm',
            type: 'node',
            id: ref
        }, function(err, body) {
            if (err) {
                console.log("Error for node/" + ref, "\n", err);
                cb(err);
            } else {
                if (!body || !body._source || !body._source.location || !body._source.location.coordinates)
                    console.log("body", body);
                cb(null, body._source.location.coordinates);
            }
        });
    }, function(err, refCoordinates) {
        if (err) {
            console.log("err", err);
            /* Don't set location */
            setImmediate(callback);
            return;
        }

        var closed = way.refs[0] === way.refs[way.refs.length - 1];
        way.location = {
            type: closed ? 'polygon' : 'linestring',
            coordinates: closed ? [refCoordinates] : refCoordinates
        };
        setImmediate(callback);
    });
};

Expander.prototype.expandRelation = function(rel, callback) {
    async.mapSeries(rel.members, function(ref, cb) {
        es.get({
            index: 'osm',
            type: ref.type,
            id: ref.id
        }, function(err, body) {
            if (err) {
                cb(err);
            } else if (body && body._source && body._source.location && body._source.location.coordinates) {
                var loc = body._source.location;
                cb(null, {
                    inner: ref.role === 'inner' || ref.role === 'enclave',
                    coordinates: loc.type == 'polygon' ? loc.coordinates : [loc.coordinates]
                });
            } else {
                cb(new Error("No location in " + ref.type + "/" + ref.id));
            }
        });
    }, function(err, refCoordinates) {
        if (err) {
            console.log("err", err);
            /* Don't set location */
            setImmediate(callback);
            return;
        }

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
                    return c.coordinates;
                })
            };
        }
        setImmediate(callback);
    });
};


util.inherits(BulkWriter, Writable);
function BulkWriter() {
    Writable.call(this, { objectMode: true, highWaterMark: 4 });
}

BulkWriter.prototype._write = function(batch, encoding, callback) {
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
}


process.stdin
    .pipe(parseOSM())
    .pipe(new Expander())
    .pipe(new BulkWriter());
