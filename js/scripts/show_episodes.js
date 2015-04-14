var AllStats, EpisodePuller, all_stats, argv, csv, csv_encoder, debug, elasticsearch, end_date, ep_end, ep_puller, ep_search_body, ep_start, es, fs, scpr_es, start_date, tz, zone,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

elasticsearch = require("elasticsearch");

csv = require("csv");

fs = require("fs");

tz = require("timezone");

debug = require("debug")("scpr");

argv = require('yargs').demand(['show', 'start', 'end']).describe({
  start: "Start Date",
  end: "End Date",
  ep_start: "Start Date for Episodes",
  ep_end: "End Date for Episodes",
  zone: "Timezone",
  verbose: "Show Debugging Logs",
  sessions: "Use Sessions (UUID)"
}).boolean(["verbose", "sessions"])["default"]({
  sessions: true,
  verbose: false,
  start: null,
  end: null,
  ep_start: null,
  ep_end: null,
  zone: "America/Los_Angeles"
}).argv;

if (argv.verbose) {
  (require("debug")).enable("scpr");
  debug = require("debug")("scpr");
}

zone = tz(require("timezone/" + argv.zone));

scpr_es = new elasticsearch.Client({
  host: "es-scpr-es.service.consul:9200"
});

es = new elasticsearch.Client({
  host: "logstash.i.scprdev.org:9200"
});

start_date = zone(argv.start, argv.zone);

end_date = zone(argv.end, argv.zone);

ep_start = argv.ep_start ? zone(argv.start, argv.zone) : start_date;

ep_end = argv.ep_end ? zone(argv.end, argv.zone) : end_date;

console.error("Stats: " + start_date + " - " + end_date);

console.error("Episodes: " + ep_start + " - " + ep_end);

AllStats = (function(_super) {
  __extends(AllStats, _super);

  function AllStats() {
    AllStats.__super__.constructor.call(this, {
      objectMode: true
    });
    this.stats = [];
    this.first_date = null;
    this.last_date = null;
  }

  AllStats.prototype._transform = function(obj, encoding, cb) {
    this.stats.push(obj);
    console.error("Stats for " + obj.key + ": ", obj.stats);
    if (!this.first_date || obj.stats.first_date < this.first_date) {
      this.first_date = obj.stats.first_date;
    }
    if (!this.last_date || obj.stats.last_date > this.last_date) {
      this.last_date = obj.stats.last_date;
    }
    return cb();
  };

  AllStats.prototype._flush = function(cb) {
    var d, k, keys, s, values, _i, _j, _len, _len1, _ref;
    keys = [];
    d = this.first_date;
    while (true) {
      keys.push(zone(d, "%Y-%m-%d", argv.zone));
      d = tz(d, "+1 day");
      if (d > this.last_date) {
        break;
      }
    }
    this.push(["key"].concat(keys));
    _ref = this.stats;
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      s = _ref[_i];
      values = [s.key];
      for (_j = 0, _len1 = keys.length; _j < _len1; _j++) {
        k = keys[_j];
        values.push(s.stats.days[k] || 0);
      }
      this.push(values);
    }
    return cb();
  };

  return AllStats;

})(require("stream").Transform);

EpisodePuller = (function(_super) {
  __extends(EpisodePuller, _super);

  function EpisodePuller() {
    EpisodePuller.__super__.constructor.call(this, {
      objectMode: true
    });
  }

  EpisodePuller.prototype._indices = function(ep_date) {
    var end, idxs, start, ts;
    idxs = [];
    start = start_date && start_date > ep_date ? start_date : ep_date;
    end = end_date ? end_date : Number(new Date());
    ts = start;
    while (true) {
      idxs.push("logstash-" + (tz(ts, "%Y.%m.%d")));
      ts = tz(ts, "+1 day");
      if (ts > end) {
        break;
      }
    }
    return idxs;
  };

  EpisodePuller.prototype._transform = function(ep, encoding, cb) {
    var body, ep_date, tz_offset;
    debug("Processing " + ep.date);
    ep_date = zone(ep.date, argv.zone);
    tz_offset = zone(ep_date, "%:z", argv.zone);
    body = {
      query: {
        constant_score: {
          filter: {
            and: [
              {
                terms: {
                  "response.raw": ["200", "206"]
                }
              }, {
                range: {
                  bytes_sent: {
                    gte: 8193
                  }
                }
              }, {
                terms: {
                  "request_path.raw": ["/audio/" + ep.file, "/podcasts/" + ep.file],
                  _cache: false
                }
              }, {
                range: {
                  "@timestamp": {
                    gte: tz(start_date, "%Y-%m-%dT%H:%M"),
                    lt: tz(end_date, "%Y-%m-%dT%H:%M")
                  }
                }
              }
            ]
          }
        }
      },
      size: 0,
      aggs: {
        dates: {
          date_histogram: {
            field: "@timestamp",
            interval: "1d",
            time_zone: tz_offset
          },
          aggs: {
            sessions: {
              cardinality: {
                field: "quuid.raw",
                precision_threshold: 100
              }
            }
          }
        }
      }
    };
    debug("Searching " + ((this._indices(ep_date)).join(",")), JSON.stringify(body));
    return es.search({
      index: this._indices(ep_date),
      body: body,
      type: "nginx"
    }, function(err, results) {
      var b, day_key, days, first_date, last_date, ts, _i, _len, _ref, _results;
      if (err) {
        console.error("ES ERROR: ", err);
        return false;
      }
      first_date = null;
      last_date = null;
      days = {};
      debug("Results is ", results);
      _ref = results.aggregations.dates.buckets;
      _results = [];
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        b = _ref[_i];
        ts = tz(key);
        _results.push(day_key = zone(key, "%Y"));
      }
      return _results;
    });
  };

  return EpisodePuller;

})(require("stream").Transform);

ep_puller = new EpisodePuller;

all_stats = new AllStats;

csv_encoder = csv.stringify();

ep_puller.pipe(all_stats).pipe(csv_encoder).pipe(process.stdout);

all_stats.once("end", function() {
  return setTimeout(function() {
    return process.exit();
  }, 500);
});

ep_search_body = {
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
          }, {
            range: {
              public_datetime: {
                gt: zone(ep_start, "%FT%T%^z"),
                lt: zone(ep_end, "%FT%T%^z")
              }
            }
          }
        ]
      }
    }
  },
  sort: [
    {
      public_datetime: {
        order: "asc"
      }
    }
  ],
  size: 100
};

scpr_es.search({
  index: "scprv4_production-articles-all",
  type: "show_episode",
  body: ep_search_body
}, function(err, results) {
  var e, file, _i, _len, _ref;
  if (err) {
    throw err;
  }
  debug("Got " + results.hits.hits.length + " episodes.");
  _ref = results.hits.hits;
  for (_i = 0, _len = _ref.length; _i < _len; _i++) {
    e = _ref[_i];
    file = e._source.audio[0].url.replace("http://media.scpr.org/audio/", "");
    ep_puller.write({
      date: e._source.public_datetime,
      file: file
    });
  }
  return ep_puller.end();
});

//# sourceMappingURL=show_episodes.js.map
