var async = require('async');
var parseOSM = require('osm-pbf-parser');
var leveldown = require('leveldown');
var db = leveldown('./osm');
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');


util.inherits(ToDB, Transform);
function ToDB() {
    Transform.call(this, { objectMode: true, highWaterMark: 1 });
    this.stats = {};
}

ToDB.prototype._transform = function(values, encoding, callback) {
    this.push(
        values.map(function(value) {
            if (!this.stats.hasOwnProperty(value.type)) {
                this.stats[value.type] = 0;
            }
            this.stats[value.type]++;

            return {
                type: 'put',
                key: value.id,
                value: JSON.stringify(value)
            };
        }.bind(this))
    );

    setImmediate(callback);
};

ToDB.prototype._flush = function(callback) {
    console.log("Processed", this.stats);
    callback();
};


util.inherits(BatchWriter, Writable);
function BatchWriter() {
    Writable.call(this, { objectMode: true, highWaterMark: 1 });
}

BatchWriter.prototype._write = function(chunk, encoding, callback) {
    db.batch(chunk, function(err) {
        if (err) {
            callback(err);
        } else {
            setImmediate(callback);
        }
    });
};


db.open(function(err) {
    if (err) {
        console.log("open", err);
        process.exit(1);
    }

    process.stdin
        .pipe(parseOSM())
        .pipe(new ToDB())
        .pipe(new BatchWriter())
        .on('end', function() {
            db.close();
        });
});
