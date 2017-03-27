#!/usr/bin/env bash
SHOWS=(the-mash-up-americans airtalk filmweek filmweek-marquee the-brood offramp take-two the-awards-show-show the-frame
        loh-down-on-science loh-life tuesday-reviewsday)

for SHOW in "${SHOWS[@]}"
do
    ./runner-show-episodes --show $SHOW --verbose > ~/Desktop/$SHOW.csv
done