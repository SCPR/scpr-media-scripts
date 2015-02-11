# scpr-media-scripts

This collection of scripts uses media data logged into Logstash / Elasticsearch
to compute download and user statistics.

## Requirements

* Node.js 0.10
* VPN access to logstash.i.scprdev.org (Datacenter Network)

## Installation

`npm install -g scpr/scpr-media-scripts`

## Scripts

### `scpr-media-show-day`

Downloads by show by day. Produces a CSV with shows and dates.

Accepts `start` and `end` parameters for date range. Optionally
can be given a `show` parameter to return data only for one show.

__NOTE:__ Accepts `type` parameter with values of `podcast` (default)
or `ondemand`, but `ondemand` is currently broken since we don't set
a UUID for non-podcast downloads.

