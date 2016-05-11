# Internet Of Flying Things

This project is a PoC whereby we succesfully test whether is possible to analyze the collective behaviour of a UAV fleet using Big Data streaming tools. Namely Spark Streaming.

## Where to start reading this code?

It seems a good a idea to start with the Spark Streaming applications where streams are created and combined in different ways to show the transformations providing detection algorithms, how the latter works and how their generated events can be persisted into Cassandra.

All these Spark Streaming drivers belong to `com.stratio.ioft.streaming.drivers` package. There are two types of apps: Demonstration drivers and a main application which covers many cases and includes persistence.

- [JustNaiveBumpDetection:](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/drivers/JustNaiveBumpDetection.scala) Uses naive detection algorithm to detect bumps.
- [JustOutliersBasedBumpDetection](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/drivers/JustOutliersBasedBumpDetection.scala) Improves the results of the previous app by using per window simple statistical analysis.
- [NormalizedOutliersBasedBumpDetection](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/drivers/NormalizedOutliersBasedBumpDetection.scala) Same as `JustOutliersBasedBumpDetection` but normalizing acceleration vectors frame of reference.

And, finally, the full-stack driver: 

- [CompleteFlowWithPersistence](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/drivers/CompleteFlowWithPersistence.scala)

## Stream sources and transformations 

The building blocks of the aforementioned Spark Streaming drivers (applications) can be found under `com.stratio.ioft.streaming.transformations` package. It contains a set of objects with functions to build, compose and transform streams:

- At [Sources](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/transformations/Sources.scala) are deffined those methods which extract high level objects from an `Entry` stream.
- [Aggregators](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/transformations/Aggregators.scala) contains methods to aggregate sets of events within a window into a smaller set of events. e.g: All actittuve events within a window can be transformed into a single history event sumarizing what happened during the window.
- [Combinators](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/transformations/Combinators.scala)
- Find at [Detectors](https://github.com/pfcoperez/sparkstream_ioft/blob/master/src/main/scala/com/stratio/ioft/streaming/transformations/Detectors.scala) the transformations needed to located x-axis bumps.

## Input

### Attitude

Provided by entries (`Entry`) with name _AttitudeState_

![yaw pitch roll](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c1/Yaw_Axis_Corrected.svg/2000px-Yaw_Axis_Corrected.svg.png "Yaw, Pitch, Roll")

The following example:


```json
{
  "fields": [
    {
      "name": "Roll", 
      "type": "float32", 
      "unit": "degrees", 
      "values": [
        {
          "name": "0", 
          "value": 12.360915184020996
        }
      ]
    }, 
    {
      "name": "Pitch", 
      "type": "float32", 
      "unit": "degrees", 
      "values": [
        {
          "name": "0", 
          "value": -7.535737991333008
        }
      ]
    }, 
    {
      "name": "Yaw", 
      "type": "float32", 
      "unit": "degrees", 
      "values": [
        {
          "name": "0", 
          "value": 13.041015625
        }
      ]
    }
  ], 
  "gcs_timestamp_ms": 1459438549760, 
  "id": "D7E0D964", 
  "instance": 0, 
  "name": "AttitudeState", 
  "setting": false
}

```

Is telling us that:

* The drone is looking towards NE: 13.04º (positive yaw)
* It is 12.36º inclined to the right (positive roll)
* And slightly facing ground: -7.53º (negative pitch)

### Acceleration

Acceleration information is given as:

```json
{
    "fields": [
      {
        "name": "x", 
        "type": "float32", 
        "unit": "m/s^2", 
        "values": [
          {
            "name": "0", 
            "value": -0.02022501826286316
          }
        ]
      }, 
      {
        "name": "y", 
        "type": "float32", 
        "unit": "m/s^2", 
        "values": [
          {
            "name": "0", 
            "value": 0.05858611315488815
          }
        ]
      }, 
      {
        "name": "z", 
        "type": "float32", 
        "unit": "m/s^2", 
        "values": [
          {
            "name": "0", 
            "value": -9.915225982666016
          }
        ]
      }
    ], 
    "gcs_timestamp_ms": 1461080146848, 
    "id": "AD3C0E06", 
    "instance": 0, 
    "name": "AccelState", 
    "setting": false
  }
```

Where:

*X Represents the horizontal acceleration in the direction of roll axis. Positive values represent the UAV accelerating fordward whereas negative values show a slowing or backward acceleraton.
*Y Represents the the horizontal acceleration in the direction of the pitch axis, that is, left (-) or right(+)
*Z stands for the vertical acceleration which, at rest, should be near -9.8 m/s^2 thus representing gravitational acceleration.


### Barometer and its magnitudes

A baromether with an internal termoter is embeeded in the flight instrumentation chip, it provides a temperature adjusted barometric measure as well which allows altitude (not attitude) inference:

```json
  {
    "fields": [
      {
        "name": "Altitude", 
        "type": "float32", 
        "unit": "m", 
        "values": [
          {
            "name": "0", 
            "value": 646.9263916015625
          }
        ]
      }, 
      {
        "name": "Temperature", 
        "type": "float32", 
        "unit": "C", 
        "values": [
          {
            "name": "0", 
            "value": 46.57999801635742
          }
        ]
      }, 
      {
        "name": "Pressure", 
        "type": "float32", 
        "unit": "kPa", 
        "values": [
          {
            "name": "0", 
            "value": 93792
          }
        ]
      }
    ], 
    "gcs_timestamp_ms": 1461080146844, 
    "id": "48120EA6", 
    "instance": 0, 
    "name": "BaroSensor", 
    "setting": false
  },
```
