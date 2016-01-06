var AllStats, EpisodePuller, all_stats, argv, csv, csv_encoder, debug, elasticsearch, end_date, ep_puller, ep_search_body, es, fs, scpr_es, start_date, tz, via, zone, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

elasticsearch = require("elasticsearch");

csv = require("csv");

fs = require("fs");

tz = require("timezone");

_ = require("underscore");

debug = require("debug")("scpr");

argv = require('yargs').demand(['show', 'start', 'end']).describe({
  start: "Start Date",
  end: "End Date",
  show: "Show slug",
  days: "Number of Days for Each Episode",
  zone: "Timezone",
  verbose: "Show Debugging Logs",
  type: "Listening Type (podcast or ondemand)",
  lidx: "Listening Index Prefix",
  size: "Request Size Floor",
  uuid: "ES Field for UUID"
}).boolean(["verbose", "sessions"]).help("help")["default"]({
  sessions: true,
  verbose: false,
  days: 7,
  zone: "America/Los_Angeles",
  type: "podcast",
  lidx: "logstash-audio",
  size: 102400,
  uuid: "quuid.raw"
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
  host: "192.168.133.25:9200"
});

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

AllStats = (function(_super) {
  __extends(AllStats, _super);

  function AllStats() {
    var d;
    AllStats.__super__.constructor.call(this, {
      objectMode: true
    });
    this.push(_.flatten([
      "Episode Date", "Episode Title", "Audio Duration", "Audio Size", (function() {
        var _i, _ref, _results;
        _results = [];
        for (d = _i = 0, _ref = argv.days; 0 <= _ref ? _i <= _ref : _i >= _ref; d = 0 <= _ref ? ++_i : --_i) {
          _results.push(["D" + (d + 1) + " Downloads", "D" + (d + 1) + " Transfer"]);
        }
        return _results;
      })()
    ]));
  }

  AllStats.prototype._transform = function(s, encoding, cb) {
    var k, values, _i, _ref, _ref1, _ref2;
    values = [zone(s.episode.date, "%Y-%m-%d", argv.zone), s.episode.title, s.episode.duration, s.episode.size];
    for (k = _i = 0, _ref = argv.days; 0 <= _ref ? _i <= _ref : _i >= _ref; k = 0 <= _ref ? ++_i : --_i) {
      values.push(((_ref1 = s.stats[k]) != null ? _ref1.downloads : void 0) || 0);
      values.push(((_ref2 = s.stats[k]) != null ? _ref2.size : void 0) || 0);
    }
    this.push(values);
    return cb();
  };

  AllStats.prototype._flush = function(cb) {
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

  EpisodePuller.prototype._indices = function(ep_date, ep_end) {
    var idxs, ts;
    idxs = [];
    ts = ep_date;
    debug("ep_end is ", ep_date, ep_end);
    while (true) {
      idxs.push("" + argv.lidx + "-" + (tz(ts, "%Y.%m.%d")));
      ts = tz(ts, "+1 day");
      if (ts > ep_end) {
        break;
      }
    }
    return idxs;
  };

  EpisodePuller.prototype._transform = function(ep, encoding, cb) {
    var body, ep_date, ep_end, tz_offset;
    debug("Processing " + ep.date);
    ep_date = zone(ep.date, argv.zone);
    ep_end = zone(ep_date, "+" + argv.days + " day");
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
                    gte: argv.size
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
                    gte: tz(ep_date, "%Y-%m-%dT%H:%M"),
                    lt: tz(ep_end, "%Y-%m-%dT%H:%M")
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
                field: argv.uuid,
                precision_threshold: 1000
              }
            },
            total_size: {
              sum: {
                field: "bytes_sent"
              }
            }
          }
        }
      }
    };
    debug("Searching " + ((this._indices(ep_date, ep_end)).join(",")), JSON.stringify(body));
    return es.search({
      index: this._indices(ep_date, ep_end),
      body: body,
      type: "nginx",
      ignoreUnavailable: true
    }, (function(_this) {
      return function(err, results) {
        var b, days, first_date, idx, last_date, stats, _i, _len, _ref, _ref1, _ref2;
        if (err) {
          console.error("ES ERROR: ", err);
          return false;
        }
        first_date = null;
        last_date = null;
        days = {};
        debug("Results is ", results);
        stats = [];
        _ref = results.aggregations.dates.buckets;
        for (idx = _i = 0, _len = _ref.length; _i < _len; idx = ++_i) {
          b = _ref[idx];
          stats[idx] = {
            downloads: ((_ref1 = b.sessions) != null ? _ref1.value : void 0) || 0,
            size: ((_ref2 = b.total_size) != null ? _ref2.value : void 0) || 0
          };
        }
        _this.push({
          episode: ep,
          stats: stats
        });
        return cb();
      };
    })(this));
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
                gt: zone(start_date, "%FT%T%^z"),
                lt: zone(end_date, "%FT%T%^z")
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
  var duration, e, file, size, _i, _len, _ref;
  if (err) {
    throw err;
  }
  debug("Got " + results.hits.hits.length + " episodes.");
  _ref = results.hits.hits;
  for (_i = 0, _len = _ref.length; _i < _len; _i++) {
    e = _ref[_i];
    file = e._source.audio[0].url.replace("http://media.scpr.org/audio/", "");
    duration = e._source.audio[0].duration;
    size = e._source.audio[0].size;
    ep_puller.write({
      date: e._source.public_datetime,
      file: file,
      title: e._source.title,
      duration: duration,
      size: size
    });
  }
  return ep_puller.end();
});

//# sourceMappingURL=show_episodes_duration.js.map
