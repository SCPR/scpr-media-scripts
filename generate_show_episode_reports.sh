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
    ./runner-show-episodes --show $SHOW --verbose $START $END> ~/Desktop/$SHOW.csv
done