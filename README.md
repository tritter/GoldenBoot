# GoldenBoot
Correctness of Big Data Twitter analyses of all tweets of the World Cup 2014.

The 66.2 million tweets of the World Cup 2014 are used to analyse the correctness of events against real world events. Because there are so many events we have decided to focus on only one kind. The kind we are interested in are goals. With these goals multiple statistics are calculated. One of the statistics that is being calculated by FIFA is the "Golden Boot" which is also officially available at [FIFA][1]. The "Golden Boot" is a price that will be given to the player that made the most goals across all matches.
[1]: http://www.fifa.com/worldcup/awards/golden-boot/index.html

## Installation

To run the map reduce jobs you need to have a kafka instance running on your machine.

### Load data

Load the tweets tsv into the kafka instance:

    hadoop fs -put res/tweets.tsv tweets.tsv

Load the player csv into the kafka instance:

    hadoop fs -put res/players.csv player.csv

### Build Jar

Go into the project directory clean & build the Jar file:

    mvn clean && mvn package

### Execute map reduce

Start the map reduce jobs:

    hadoop jar bigdata-0.1.jar nl.utwente.bigdata.GoldenBoot tweets*.tsv

### Print result

Print and sort the result:

    hadoop fs -cat golden_boot_output/part* | sort -t$'\t' -nrk2