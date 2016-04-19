# Internet Of Flying Things

## Elasticsearch commands

### Create index
curl -XPUT 'http://localhost:9200/ioft/'

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
        }l
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

```
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
