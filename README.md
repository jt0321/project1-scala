# Project 1

## A simple job for Spark... with Scala

A revision of [project1](https://github.com/200413-java-spark/project-1-jeremy) in Scala
TODO: persistence with SQL (Slick); spec

This project presents a simple analysis of crimes in Chicago in 2015 using Apache Spark.
Original data set can be found at <https://data.cityofchicago.org/Public-Safety/Crimes-2015/vwwp-7yr9>.
It contains 264,384 rows and 22 columns.

### Build

```bash
> sbt compile
```

### Run

Spark Akka Http Server

```bash
> sbt run
```

#### HTTP methods

##### GET

```bash
> curl http://localhost:8888/spark

ASPECTS OF CRIME! ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description,
Arrest, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate,
Y Coordinate, Year, Updated On, Latitude, Longitude, Location
```

##### POST

Use up to two parameter values of type "cat" to get a list of crime in descending
order by counts and grouped by the categories requested. Set "save" to true to write to
H2.

```bash
> curl -X POST "localhost:8080/spark?cat=Primary%20Type&cat=Location%20Description&save=true"

Location Description, Primary Type
STREET, THEFT: 14451
APARTMENT, BATTERY: 12291
STREET, CRIMINAL DAMAGE: 10488
RESIDENCE, BATTERY: 9237
SIDEWALK, BATTERY: 7867
...
```
