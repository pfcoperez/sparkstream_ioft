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

