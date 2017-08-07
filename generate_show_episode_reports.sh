#!/usr/bin/env bash
SHOWS=(the-mash-up-americans airtalk filmweek filmweek-marquee the-brood offramp take-two the-awards-show-show the-frame
        loh-down-on-science loh-life tuesday-reviewsday)

for i in "$@"
do
    case $i in
        --start=*)
        START="--start ${i#*=}"
        shift
        ;;
        --end=*)
        END="--end ${i#*=}"
        shift
        ;;
    esac
done

START=${START- ``}
END=${END- ``}

for SHOW in "${SHOWS[@]}"
do
    $RUNNER_SCRIPT --show $SHOW --verbose $START $END> $REPORTS_DIR/$SHOW.csv
done