elasticsearch   = require "elasticsearch"
csv             = require "csv"
fs              = require "fs"
tz              = require "timezone"
_ = require "underscore"

debug           = require("debug")("scpr")

argv = require('yargs')
    .demand(['start','end'])
    .describe
        show:       "Show Key (Default is all shows)"
        type:       "Listening Type (podcast or ondemand)"
        start:      "Start Date"
        end:        "End Date"
        zone:       "Timezone"
        verbose:    "Show Debugging Logs"
    .boolean(['verbose'])
    .default
        verbose:    false
        type:       "podcast"
        zone:       "America/Los_Angeles"
    .argv

if argv.verbose
    (require "debug").enable("scpr")
    debug = require("debug")("scpr")

zone = tz(require("timezone/#{argv.zone}"))

es = new elasticsearch.Client host:"logstash.i.scprdev.org:9200"

start_date  = zone(argv.start,argv.zone)
end_date    = zone(argv.end,argv.zone)

via = switch argv.type
    when "podcast"
        ["podcast"]
    when "ondemand"
        ["api","website","ondemand"]
    else
        console.error "Invalid type argument."
        process.exit()

# For each day in our period, run a query to get sessions by context

class DayPuller extends require("stream").Transform
    constructor: ->
        super objectMode:true

    _transform: (date,encoding,cb) ->
        debug "Running #{zone(date,argv.zone,"%Y.%m.%d")}"

        # Since our logstash data is stored in indices named via UTC, we
        # always want to our date + the next date

        tomorrow = tz(date,"+1 day")

        indices = [
            "logstash-#{zone(date,argv.zone,"%Y.%m.%d")}",
            "logstash-#{zone(tomorrow,argv.zone,"%Y.%m.%d")}"
        ]

        debug "Indices is ", indices

        filters = [
            terms: { qvia: via }
        ,
            range: { bytes_sent: { gte: 8192 } }
        ,
            range:
                "@timestamp":
                    gte:    tz(date,"%Y-%m-%dT%H:%M")
                    lt:     tz(tomorrow,"%Y-%m-%dT%H:%M")
        ]

        if argv.show
            filters.push term:{ "qcontext.raw":argv.show }

        body =
            query:
                constant_score:
                    filter:
                        and:filters
            size: 0
            aggs:
                show:
                    terms:
                        field:  "qcontext.raw"
                        size:   20
                    aggs:
                        sessions:
                            cardinality:
                                field:                  "quuid.raw"
                                precision_threshold:    100

        debug "Body is ", JSON.stringify(body)

        es.search index:indices, type:"nginx", body:body, (err,results) =>
            if err
                throw err

            debug "Results is ", results

            shows = {}
            for b in results.aggregations.show.buckets
                shows[ b.key ] = b.sessions.value

            @push date:date, shows:shows

            cb()

#----------

class CSVFormatter extends require("stream").Transform
    constructor: ->
        super objectMode:true
        @shows = {}
        @dates = []

    _transform: (obj,encoding,cb) ->
        for show,value of obj.shows
            @shows[show] = 0 if !@shows[show]
            @shows[show] += value

        @dates.push obj

        cb()

    _flush: (cb) ->
        sorted = _.sortBy Object.keys(@shows), ((s) => -@shows[s])

        @push ["Date",(s for s in sorted)...].join(",") + "\n"

        for d in @dates
            @push [zone(d.date,argv.zone,"%Y.%m.%d"),( d.shows[s] for s in sorted )...].join(",") + "\n"

        cb()

day_puller = new DayPuller()
csv = new CSVFormatter()

day_puller.pipe(csv).pipe(process.stdout)

ts = start_date

loop
    day_puller.write(ts)
    ts = tz(ts,"+1 day")
    break if ts >= end_date

day_puller.end()

csv.on "finish", =>
    debug "Finished"
    process.exit()
