var Aggregator, DownloadsPuller, aggregator, argv, csv, debug, downloads_puller, end_date, es, filenameToEpisode, fs, moment, scpr_es, start_date, tz, via, zone, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

csv = require("csv");

fs = require("fs");

tz = require("timezone");

moment = require("moment");

debug = require("debug")("scpr");

scpr_es = require("./elasticsearch_connections").scpr_es;

es = require("./elasticsearch_connections").es_client;

_ = require("underscore");

argv = require('yargs').demand(['show']).describe({
  start: "Start Date",
  end: "End Date",
  show: "Show slug",
  days: "Number of Days for Each Episode",
  zone: "Timezone",
  verbose: "Show Debugging Logs",
  type: "Listening Type (podcast or ondemand)",
  lidx: "Listening Index Prefix",
  size: "Request Size Floor",
  uuid: "ES Field for UUID",
  prefix: "Index Prefix"
}).boolean(["verbose", "sessions"]).help("help")["default"]({
  start: new moment().subtract(1, 'months').date(1),
  end: new moment().date(1).subtract(1, 'day'),
  sessions: true,
  verbose: false,
  days: 30,
  zone: "America/Los_Angeles",
  type: "podcast",
  lidx: "logstash-audio",
  size: 204800,
  uuid: "synth_uuid2.raw",
  prefix: "logstash-audio"
}).argv;

if (argv.verbose) {
  (require("debug")).enable("scpr");
  debug = require("debug")("scpr");
}

zone = tz(require("timezone/" + argv.zone));

start_date = zone(argv.start, argv.zone);

end_date = zone(argv.end, argv.zone);

console.error("Episodes: " + start_date + " - " + end_date);

via = (function() {
  switch (argv.type) {
    case "podcast":
      return ["podcast"];
    case "ondemand":
      return ["api", "website", "ondemand"];
    default:
      console.error("Invalid type argument.");
      return process.exit();
  }
})();

filenameToEpisode = new Promise(function(resolve, reject) {
  var filename_to_episodes, scpr_es_body;
  scpr_es_body = {
    query: {
      filtered: {
        query: {
          match_all: {}
        },
        filter: {
          and: [
            {
              term: {
                "show.slug": argv.show
              }
            }, {
              term: {
                "published": true
              }
            }, {
              exists: {
                "field": "audio.url"
              }
            }
          ]
        }
      }
    },
    size: 100
  };
  filename_to_episodes = {};
  return scpr_es.search({
    index: "scprv4_production-articles-all",
    type: "show_episode",
    body: scpr_es_body
  }, function(err, results) {
    var e, file, _i, _len, _ref;
    if (err) {
      throw err;
    }
    debug("Got " + results.hits.hits.length + " episodes.");
    _ref = results.hits.hits;
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      e = _ref[_i];
      file = e._source.audio[0].url.match(/([^\/]+\.mp3)/)[0];
      filename_to_episodes[file] = e._source.title || file;
    }
    return resolve(filename_to_episodes);
  });
});

DownloadsPuller = (function(_super) {
  __extends(DownloadsPuller, _super);

  function DownloadsPuller() {
    DownloadsPuller.__super__.constructor.call(this, {
      objectMode: true
    });
  }

  DownloadsPuller.prototype._indices = function(start) {
    var end;
    end = zone(tz(start, "+1 day"), "%Y.%m.%d");
    start = zone(start, "%Y.%m.%d");
    return ["" + argv.prefix + "-" + start, "" + argv.prefix + "-" + end];
  };

  DownloadsPuller.prototype._transform = function(obj, encoding, cb) {
    var aggs, body, date, filename_to_episodes, filters, tomorrow;
    date = obj.ts;
    filename_to_episodes = obj.filename_to_episodes;
    tomorrow = tz(date, "+1 day");
    filters = [
      {
        term: {
          "nginx_host.raw": "media.scpr.org"
        }
      }, {
        terms: {
          qvia: via
        }
      }, {
        range: {
          bytes_sent: {
            gte: argv.size
          }
        }
      }, {
        range: {
          "@timestamp": {
            gte: tz(date, "%Y-%m-%dT%H:%M"),
            lt: tz(tomorrow, "%Y-%m-%dT%H:%M")
          }
        }
      }
    ];
    filters.push({
      terms: {
        "qcontext.raw": argv.show.split(","),
        execution: "bool"
      }
    });
    aggs = {
      filename: {
        terms: {
          field: "request_path.raw",
          size: 20
        },
        aggs: {
          sessions: {
            cardinality: {
              field: argv.uuid,
              precision_threshold: 1000
            }
          }
        }
      }
    };
    body = {
      query: {
        constant_score: {
          filter: {
            and: filters
          }
        }
      },
      size: 0,
      aggs: aggs
    };
    debug('Indices are: ', this._indices(date));
    return es.search({
      index: this._indices(date),
      type: "nginx",
      body: body,
      ignoreUnavailable: true
    }, (function(_this) {
      return function(err, results) {
        var b, episode, filenames, stripped_filename, _i, _len, _ref, _ref1;
        if (err) {
          throw err;
        }
        debug("Results is ", results);
        filenames = {};
        _ref1 = (_ref = results.aggregations) != null ? _ref.filename.buckets : void 0;
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          b = _ref1[_i];
          if (!b.key.match(/\/(?:podcasts|audio)\/upload\//)) {
            next;
          }
          stripped_filename = b.key.match(/([^\/]+\.mp3)/)[0];
          if (stripped_filename) {
            episode = stripped_filename;
            if (filename_to_episodes[stripped_filename]) {
              episode = filename_to_episodes[stripped_filename];
            }
            filenames[episode] = b.sessions.value;
          }
        }
        debug("date", zone(tz(date), argv.zone, "%Y/%m/%d"));
        _this.push({
          date: date,
          filenames: filenames
        });
        return cb();
      };
    })(this));
  };

  return DownloadsPuller;

})(require("stream").Transform);

Aggregator = (function(_super) {
  __extends(Aggregator, _super);

  function Aggregator() {
    Aggregator.__super__.constructor.call(this, {
      objectMode: true
    });
    this.filenames = {};
    this.dates = [];
  }

  Aggregator.prototype._transform = function(obj, encoding, cb) {
    var filename, _i, _len, _ref;
    _ref = Object.keys(obj.filenames);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      filename = _ref[_i];
      if (!this.filenames[filename]) {
        this.filenames[filename] = {};
      }
      if (!this.filenames[filename][obj.date]) {
        this.filenames[filename][obj.date] = 0;
      }
      this.filenames[filename][obj.date] += obj.filenames[filename];
    }
    this.dates.push(obj.date);
    return cb();
  };

  Aggregator.prototype._flush = function(cb) {
    var date, file, row, sorted_dates, total, _i, _j, _len, _len1, _ref;
    sorted_dates = _.sortBy(this.dates);
    this.push(["Filename"].concat(_.map(sorted_dates, function(d) {
      return zone(tz(d), "%Y/%m/%d");
    })).concat('total'));
    _ref = Object.keys(this.filenames);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      file = _ref[_i];
      row = [];
      total = 0;
      for (_j = 0, _len1 = sorted_dates.length; _j < _len1; _j++) {
        date = sorted_dates[_j];
        if (this.filenames[file][date]) {
          row.push(this.filenames[file][date]);
          total += this.filenames[file][date];
        } else {
          row.push(0);
        }
      }
      row.push(total);
      row.push("\n");
      this.push(JSON.stringify(file).concat(',').concat(row));
    }
    return cb();
  };

  return Aggregator;

})(require("stream").Transform);

downloads_puller = new DownloadsPuller;

aggregator = new Aggregator;

downloads_puller.pipe(aggregator).pipe(csv.stringify()).pipe(process.stdout);

filenameToEpisode.then(function(filename_to_episodes) {
  var ts;
  ts = start_date;
  while (true) {
    downloads_puller.write({
      ts: ts,
      filename_to_episodes: filename_to_episodes
    });
    ts = zone(ts, "+1 day");
    if (ts >= end_date) {
      break;
    }
  }
  downloads_puller.end();
  return aggregator.on("finish", (function(_this) {
    return function() {
      debug("Finished");
      return process.exit();
    };
  })(this));
});

//# sourceMappingURL=show_episodes.js.map
