package com.stratio.ioft.serialization.json4s

import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import org.json4s.JsonAST._
import org.json4s.{CustomSerializer, JArray}

object FieldSerializer extends CustomSerializer[Field]( format => (
  {  // Extractor
    case JObject(
      List(
        JField("name", JString(name)),
        JField("type", JString(tpe)),
        JField("unit", JString(unit)),
        JField("values", JArray(values: List[JObject @unchecked]))
      )
    ) =>
      implicit val _ = format
      Field(name, tpe, unit, values.map(_.extract[Value]))
  },
  PartialFunction.empty // Serializer
  )
)

object EntrySerializer extends CustomSerializer[Entry]( format => (
  // Extractor
  {
    case JObject(
      List(
        JField("fields", JArray(fields: List[JObject @unchecked])),
        JField("gcs_timestamp_ms", JInt(timestamp)),
        JField("id", JString(id)),
        JField("instance", JLong(instance)),
        JField("name", JString(name)),
        JField("setting", JBool(setting))
      )
    ) =>
      implicit val _ = format
      Entry(fields.map(_.extract[Field]), timestamp, id, instance, name, setting)
  },
  PartialFunction.empty
  )
)