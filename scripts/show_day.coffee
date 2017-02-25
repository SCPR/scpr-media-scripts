csv             = require "csv"
fs              = require "fs"
tz              = require "timezone"
_ = require "underscore"

debug           = require("debug")("scpr")

es = require("./elasticsearch_connections").es_client

argv = require('yargs')
    .demand(['start','end'])
    .describe
        show:       "Show Key (Default is all shows)"
        exclude:    "Show(s) to exclude"
        type:       "Listening Type (podcast or ondemand)"
        start:      "Start Date"
        end:        "End Date"
        zone:       "Timezone"
        verbose:    "Show Debugging Logs"
        bots:       "Include Known Bot Traffic?"
        ua:         "Limit User Agents"
        prefix:     "Index Prefix"
        size:       "Request Size Floor"
        uuid:       "ES Field for UUID"
        server:     "ES Server"
        untagged:   "Include untagged requests?"
    .boolean(['verbose','bots'])
    .help("help")
    .default
        prefix:     "logstash-audio"
        verbose:    false
        bots:       false
        type:       "podcast"
        zone:       "America/Los_Angeles"
        size:       204800
        uuid:       "synth_uuid2.raw"
        server:     es
        untagged:   false
    .argv

if argv.verbose
    (require "debug").enable("scpr")
    debug = require("debug")("scpr")

zone = tz(require("timezone/#{argv.zone}"))

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
            "#{argv.prefix}-#{zone(date,argv.zone,"%Y.%m.%d")}",
            "#{argv.prefix}-#{zone(tomorrow,argv.zone,"%Y.%m.%d")}"
        ]

        debug "Indices is ", indices

        filters = [
            term: { "nginx_host.raw": "media.scpr.org" }
        ,
            terms: { qvia: via }
        ,
            range: { bytes_sent: { gte: argv.size } }
        ,
            range:
                "@timestamp":
                    gte:    tz(date,"%Y-%m-%dT%H:%M")
                    lt:     tz(tomorrow,"%Y-%m-%dT%H:%M")
        ]

        if !argv.bots
            filters.push
                not:
                    terms:
                        "clientip.raw": ["217.156.156.69"]

            #filters.push
            #    not:
            #        terms:
            #            "agent.raw": ["Python-urllib/2.7"]

            filters.push
                not:
                    exists:
                        field: "bot"

        if argv.ua
            filters.push
                prefix:
                    "agent.raw": argv.ua


        if argv.show
            filters.push terms:
                "qcontext.raw": argv.show.split(",")
                execution:      "bool"

        if argv.exclude
            filters.push
                not:
                    terms:
                        "qcontext.raw": argv.exclude.split(",")
                        execution:      "bool"

        aggs =
            show:
                terms:
                    field:  "qcontext.raw"
                    size:   20
                aggs:
                    sessions:
                        cardinality:
                            field:                  argv.uuid
                            precision_threshold:    1000

        if argv.untagged
            aggs.sessions =
                cardinality:
                    field:                  argv.uuid
                    precision_threshold:    1000

        body =
            query:
                constant_score:
                    filter:
                        and:filters
            size: 0
            aggs: aggs

        debug "Body is ", JSON.stringify(body)

        es.search index:indices, type:"nginx", body:body, ignoreUnavailable:true, (err,results) =>
            if err
                throw err

            debug "Results is ", results

            shows = {}

            tagged_total = 0

            for b in results.aggregations?.show.buckets
                shows[ b.key ] = b.sessions.value
                tagged_total += b.sessions.value

            if argv.untagged
                shows.untagged = results.aggregations.sessions.value - tagged_total

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
    ts = zone(ts,argv.zone,"+1 day")
    break if ts >= end_date

day_puller.end()

csv.on "finish", =>
    debug "Finished"
    process.exit()
