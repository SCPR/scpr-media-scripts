csv             = require "csv"
fs              = require "fs"
tz              = require "timezone"

debug           = require("debug")("scpr")
scpr_es = require("./elasticsearch_connections").scpr_es
es = require("./elasticsearch_connections").es_client


argv = require('yargs')
    .demand(['show','start','end'])
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
    .boolean(["verbose","sessions"])
    .help("help")
    .default
        sessions:   true
        verbose:    false
        days:       7
        zone:       "America/Los_Angeles"
        type:       "podcast"
        lidx:       "logstash-audio"
        size:       204800
        uuid:       "synth_uuid2.raw"
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

class AllStats extends require("stream").Transform
    constructor: ->
        super objectMode:true

        @push ["Episode Date","Episode Title"].concat( "Day #{d+1}" for d in [0..argv.days] )

    _transform: (s,encoding,cb) ->
        values = [zone(s.episode.date,"%Y-%m-%d",argv.zone),s.episode.title]

        for k in [0..argv.days]
            values.push s.stats[k] || 0

        @push values

        cb()

    _flush: (cb) ->
        cb()


#----------

# Given an episode, pull stats about its performance across our date range

class EpisodePuller extends require("stream").Transform
    constructor: ->
        super objectMode:true

    #----------

    _indices: (ep_date,ep_end) ->
        idxs = []

        # starting with ep_date or start_date (whichever is later), list each
        # day for `days` days

        ts = ep_date

        debug "ep_end is ", ep_date,ep_end

        loop
            idxs.push "#{argv.lidx}-#{tz(ts,"%Y.%m.%d")}"
            ts = tz(ts,"+1 day")
            break if ts > ep_end

        return idxs

    #----------

    _transform: (ep,encoding,cb) ->

        # -- prepare our query -- #

        debug "Processing #{ ep.date }"

        ep_date = zone(ep.date,argv.zone)

        ep_end = zone(ep_date,"+#{argv.days} day")

        tz_offset = zone(ep_date,"%:z",argv.zone)

        body =
            query:
                constant_score:
                    filter:
                        and:[
                            terms:
                                "response.raw":["200","206"]
                        ,
                            range:
                                bytes_sent:
                                    gte: argv.size
                        ,
                            terms:
                                "request_path.raw":["/audio/#{ep.file}","/podcasts/#{ep.file}"]
                                _cache: false
                        ,
                            range:
                                "@timestamp":
                                    gte:    tz(ep_date,"%Y-%m-%dT%H:%M")
                                    lt:     tz(ep_end,"%Y-%m-%dT%H:%M")
                        ]
            size: 0
            aggs:
                dates:
                    date_histogram:
                        field:      "@timestamp"
                        interval:   "1d"
                        time_zone:  tz_offset
                    aggs:
                        sessions:
                            cardinality:
                                field:                  "quuid.raw"
                                precision_threshold:    1000

        debug "XXXXX Searching #{ (@_indices(ep_date,ep_end)).join(",") }", JSON.stringify(body)

        es.search index:@_indices(ep_date,ep_end), body:body, type:"nginx", ignoreUnavailable:true, (err,results) =>
            if err
                console.error "ES ERROR: ", err
                return false

            first_date = null
            last_date = null
            days = {}

            debug "Results is ", results

            stats = []
            for b,idx in results.aggregations.dates.buckets
                stats[idx] = b.sessions?.value || 0

            @push episode:ep, stats:stats

            cb()

#----------

ep_puller = new EpisodePuller
all_stats = new AllStats

csv_encoder = csv.stringify()

ep_puller.pipe(all_stats).pipe(csv_encoder).pipe(process.stdout)
all_stats.once "end", ->
    setTimeout ->
        process.exit()
    , 500

# -- What episodes? -- #

# We pull episodes from the SCPRv4 Logstash instance. We're looking for
# show_episode objects in our date range that are published, have audio,
# and belong to our show.

# FIXME: If we ever intend to support longer date ranges, we would need to
# up the size on this or add a scroll

ep_search_body =
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
                ,
                    range: { public_datetime: {
                        gt: zone(start_date,"%FT%T%^z"),
                        lt: zone(end_date,"%FT%T%^z")
                    }}
                ]
            }
    sort: [{public_datetime:{order:"asc"}}],
    size: 100

scpr_es.search index:"scprv4_production-articles-all", type:"show_episode", body:ep_search_body, (err,results) ->
    throw err if err

    debug "Got #{ results.hits.hits.length } episodes."

    for e in results.hits.hits
        # Sanitize the audio file path, so that we can support both on-demand
        # and podcast listening
        file = e._source.audio[0].url.replace("http://media.scpr.org/audio/","")

        # Write the episode into the episode puller stream
        ep_puller.write date:e._source.public_datetime, file:file, title:e._source.title

    ep_puller.end()
