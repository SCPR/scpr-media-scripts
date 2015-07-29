var CSVFormatter, DayPuller, argv, csv, day_puller, debug, elasticsearch, end_date, es, fs, start_date, ts, tz, via, zone, _,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  __slice = [].slice;

elasticsearch = require("elasticsearch");

csv = require("csv");

fs = require("fs");

tz = require("timezone");

_ = require("underscore");

debug = require("debug")("scpr");

argv = require('yargs').demand(['start', 'end']).describe({
  show: "Show Key (Default is all shows)",
  type: "Listening Type (podcast or ondemand)",
  start: "Start Date",
  end: "End Date",
  zone: "Timezone",
  verbose: "Show Debugging Logs",
  bots: "Include Known Bot Traffic?",
  ua: "Limit User Agents",
  prefix: "Index Prefix",
  size: "Request Size Floor"
}).boolean(['verbose', 'bots'])["default"]({
  prefix: "logstash",
  verbose: false,
  bots: false,
  type: "podcast",
  zone: "America/Los_Angeles",
  size: 8192
}).argv;

if (argv.verbose) {
  (require("debug")).enable("scpr");
  debug = require("debug")("scpr");
}

zone = tz(require("timezone/" + argv.zone));

es = new elasticsearch.Client({
  host: "es-scpr-logstash.service.consul:9200"
});

start_date = zone(argv.start, argv.zone);

end_date = zone(argv.end, argv.zone);

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

DayPuller = (function(_super) {
  __extends(DayPuller, _super);

  function DayPuller() {
    DayPuller.__super__.constructor.call(this, {
      objectMode: true
    });
  }

  DayPuller.prototype._transform = function(date, encoding, cb) {
    var body, filters, indices, tomorrow;
    debug("Running " + (zone(date, argv.zone, "%Y.%m.%d")));
    tomorrow = tz(date, "+1 day");
    indices = ["" + argv.prefix + "-" + (zone(date, argv.zone, "%Y.%m.%d")), "" + argv.prefix + "-" + (zone(tomorrow, argv.zone, "%Y.%m.%d"))];
    debug("Indices is ", indices);
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
    if (!argv.bots) {
      filters.push({
        not: {
          terms: {
            "clientip.raw": ["217.156.156.69"]
          }
        }
      });
    }
    if (argv.ua) {
      filters.push({
        prefix: {
          "agent.raw": argv.ua
        }
      });
    }
    if (argv.show) {
      filters.push({
        term: {
          "qcontext.raw": argv.show
        }
      });
    }
    body = {
      query: {
        constant_score: {
          filter: {
            and: filters
          }
        }
      },
      size: 0,
      aggs: {
        show: {
          terms: {
            field: "qcontext.raw",
            size: 20
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
    debug("Body is ", JSON.stringify(body));
    return es.search({
      index: indices,
      type: "nginx",
      body: body
    }, (function(_this) {
      return function(err, results) {
        var b, shows, _i, _len, _ref;
        if (err) {
          throw err;
        }
        debug("Results is ", results);
        shows = {};
        _ref = results.aggregations.show.buckets;
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          b = _ref[_i];
          shows[b.key] = b.sessions.value;
        }
        _this.push({
          date: date,
          shows: shows
        });
        return cb();
      };
    })(this));
  };

  return DayPuller;

})(require("stream").Transform);

CSVFormatter = (function(_super) {
  __extends(CSVFormatter, _super);

  function CSVFormatter() {
    CSVFormatter.__super__.constructor.call(this, {
      objectMode: true
    });
    this.shows = {};
    this.dates = [];
  }

  CSVFormatter.prototype._transform = function(obj, encoding, cb) {
    var show, value, _ref;
    _ref = obj.shows;
    for (show in _ref) {
      value = _ref[show];
      if (!this.shows[show]) {
        this.shows[show] = 0;
      }
      this.shows[show] += value;
    }
    this.dates.push(obj);
    return cb();
  };

  CSVFormatter.prototype._flush = function(cb) {
    var d, s, sorted, _i, _len, _ref;
    sorted = _.sortBy(Object.keys(this.shows), ((function(_this) {
      return function(s) {
        return -_this.shows[s];
      };
    })(this)));
    this.push(["Date"].concat(__slice.call(((function() {
        var _i, _len, _results;
        _results = [];
        for (_i = 0, _len = sorted.length; _i < _len; _i++) {
          s = sorted[_i];
          _results.push(s);
        }
        return _results;
      })()))).join(",") + "\n");
    _ref = this.dates;
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      d = _ref[_i];
      this.push([zone(d.date, argv.zone, "%Y.%m.%d")].concat(__slice.call(((function() {
          var _j, _len1, _results;
          _results = [];
          for (_j = 0, _len1 = sorted.length; _j < _len1; _j++) {
            s = sorted[_j];
            _results.push(d.shows[s]);
          }
          return _results;
        })()))).join(",") + "\n");
    }
    return cb();
  };

  return CSVFormatter;

})(require("stream").Transform);

day_puller = new DayPuller();

csv = new CSVFormatter();

day_puller.pipe(csv).pipe(process.stdout);

ts = start_date;

while (true) {
  day_puller.write(ts);
  ts = tz(ts, "+1 day");
  if (ts >= end_date) {
    break;
  }
}

day_puller.end();

csv.on("finish", (function(_this) {
  return function() {
    debug("Finished");
    return process.exit();
  };
})(this));

//# sourceMappingURL=show_day.js.map
