csv             = require "csv"
fs              = require "fs"
tz              = require "timezone"
moment          = require "moment"
debug           = require("debug")("scpr")
scpr_es = require("./elasticsearch_connections").scpr_es
es = require("./elasticsearch_connections").es_client
_ = require "underscore"


argv = require('yargs')
    .demand(['show'])
    .describe
        start:      "Start Date"
        end:        "End Date"
        show:       "Show slug"
        days:       "Number of Days for Each Episode"
        zone:       "Timezone"
        verbose:    "Show Debugging Logs"
        type:       "Listening Type (podcast or ondemand)"
        lidx:       "Listening Index Prefix"
        size:       "Request Size Floor"
        uuid:       "ES Field for UUID"
        prefix:     "Index Prefix"
    .boolean(["verbose","sessions"])
    .help("help")
    .default
        start: new moment().subtract(1, 'months').date(0)
        end: new moment().date(1).subtract(1, 'day')
        sessions:   true
        verbose:    false
        days:       30
        zone:       "America/Los_Angeles"
        type:       "podcast"
        lidx:       "logstash-audio"
        size:       204800
        uuid:       "synth_uuid2.raw"
        prefix:     "logstash-audio"
    .argv

if argv.verbose
    (require "debug").enable("scpr")
    debug = require("debug")("scpr")

zone = tz(require("timezone/#{argv.zone}"))

start_date  = zone(argv.start,argv.zone)
end_date    = zone(argv.end,argv.zone)

console.error "Episodes: #{ start_date } - #{ end_date }"

via = switch argv.type
    when "podcast"
        ["podcast"]
    when "ondemand"
        ["api","website","ondemand"]
    else
        console.error "Invalid type argument."
        process.exit()

#----------

# Gets all of the episodes for this show and builds a hash so we can match the filename to the episode title
filenameToEpisode = new Promise (resolve, reject) ->
    scpr_es_body =
        query:
            filtered:
                query: { match_all:{} }
                filter: {
                    and: [
                        term: { "show.slug": argv.show },
                    ,
                        term: { "published": true },
                    ,
                        exists: { "field": "audio.url"},
                    ]
                }
        size: 100
    filename_to_episodes = {}
    scpr_es.search index:"scprv4_production-articles-all", type:"show_episode", body:scpr_es_body, (err,results) ->
        throw err if err
        debug "Got #{ results.hits.hits.length } episodes."
        for e in results.hits.hits
            file = e._source.audio[0].url.match(/([^\/]+\.mp3)/)[0]
            filename_to_episodes[file] = e._source.title || file
        resolve(filename_to_episodes)

# Gets all of the timeframe's downloads
class DownloadsPuller extends require("stream").Transform
    constructor: ->
        super objectMode:true

    _indices: (start) ->
        end = zone(tz(start,"+1 day"), "%Y.%m.%d")
        start = zone(start, "%Y.%m.%d")
        return [
            "#{argv.prefix}-#{start}"
            "#{argv.prefix}-#{end}"
        ]

    _transform: (obj,encoding,cb) ->
        date = obj.ts
        filename_to_episodes = obj.filename_to_episodes
        tomorrow = tz(date,"+1 day")

        filters =  [
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

        filters.push terms:
            "qcontext.raw": argv.show.split(",")
            execution:      "bool"

        aggs =
            filename:
                terms:
                    field:  "request_path.raw"
                    size:   20
                aggs:
                    sessions:
                        cardinality:
                            field:                  argv.uuid
                            precision_threshold:    1000

        body =
            query:
                constant_score:
                    filter:
                        and: filters
            size: 0
            aggs: aggs
        debug 'Indices are: ', @_indices(date)

        es.search index:@_indices(date), type:"nginx", body:body, ignoreUnavailable:true, (err,results) =>
            if err
                throw err

            debug "Results is ", results

            filenames = {}

            for b in results.aggregations?.filename.buckets
                break if !b.key.match(/\/(?:podcasts|audio)\//)
                stripped_filename = b.key.match(/([^\/]+\.mp3)/)[0]
                if stripped_filename
                    episode = stripped_filename
                    if filename_to_episodes[stripped_filename]
                        episode = filename_to_episodes[stripped_filename]
                    filenames[ episode ] = b.sessions.value
            debug "date", zone(tz(date), argv.zone, "%Y/%m/%d")
            @push date:date, filenames: filenames

            cb()

# Aggregates the downloads, matches it to the episode name, and formats it into rows of episodes and columns of days
class Aggregator extends require("stream").Transform
    constructor: ->
        super objectMode:true
        @filenames = {}
        @dates = []

    _transform: (obj,encoding,cb) ->
        for filename in Object.keys(obj.filenames)
            @filenames[filename] = {} if !@filenames[filename]
            @filenames[filename][obj.date] = 0 if !@filenames[filename][obj.date]
            @filenames[filename][obj.date] += obj.filenames[filename]
        @dates.push obj.date
        cb()

    _flush: (cb) ->
        sorted_dates = _.sortBy(@dates)
        @push ["Filename"].concat(_.map(sorted_dates, (d) -> zone(tz(d), "%Y/%m/%d"))).concat('total')
        for file in Object.keys(@filenames)
            row = []
            total = 0
            for date in sorted_dates
                if @filenames[file][date]
                    row.push @filenames[file][date]
                    total += @filenames[file][date]
                else
                    row.push 0
            row.push total
            row.push "\n"
            @push JSON.stringify(file).concat(',').concat(row)
        cb()

downloads_puller = new DownloadsPuller
aggregator = new Aggregator
downloads_puller.pipe(aggregator).pipe(csv.stringify()).pipe(process.stdout)

filenameToEpisode.then((filename_to_episodes) ->
    ts = zone(start_date, "+1 day")
    loop
        downloads_puller.write(ts: ts, filename_to_episodes: filename_to_episodes)
        ts = zone(ts,"+1 day")
        break if ts >= end_date

    downloads_puller.end()
    aggregator.on "finish", =>
        debug "Finished"
        process.exit()
)
