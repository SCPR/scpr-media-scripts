var AllStats, EpisodePuller, all_stats, argv, csv, csv_encoder, debug, end_date, ep_puller, ep_search_body, es, fs, scpr_es, start_date, tz, via, zone,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

csv = require("csv");

fs = require("fs");

tz = require("timezone");

debug = require("debug")("scpr");

scpr_es = require("./elasticsearch_connections").scpr_es;

es = require("./elasticsearch_connections").es_client;

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
  size: 204800,
  uuid: "synth_uuid2.raw"
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

AllStats = (function(_super) {
  __extends(AllStats, _super);

  function AllStats() {
    var d;
    AllStats.__super__.constructor.call(this, {
      objectMode: true
    });
    this.push(["Episode Date", "Episode Title"].concat((function() {
      var _i, _ref, _results;
      _results = [];
      for (d = _i = 0, _ref = argv.days; 0 <= _ref ? _i <= _ref : _i >= _ref; d = 0 <= _ref ? ++_i : --_i) {
        _results.push("Day " + (d + 1));
      }
      return _results;
    })()));
  }

  AllStats.prototype._transform = function(s, encoding, cb) {
    var k, values, _i, _ref;
    values = [zone(s.episode.date, "%Y-%m-%d", argv.zone), s.episode.title];
    for (k = _i = 0, _ref = argv.days; 0 <= _ref ? _i <= _ref : _i >= _ref; k = 0 <= _ref ? ++_i : --_i) {
      values.push(s.stats[k] || 0);
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
                field: "quuid.raw",
                precision_threshold: 1000
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
        var b, days, first_date, idx, last_date, stats, _i, _len, _ref, _ref1;
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
          stats[idx] = ((_ref1 = b.sessions) != null ? _ref1.value : void 0) || 0;
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
      file: file,
      title: e._source.title
    });
  }
  return ep_puller.end();
});

//# sourceMappingURL=show_episodes.js.map
