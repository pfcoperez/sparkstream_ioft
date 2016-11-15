package com.stratio.ioft.serialization.json4s

import com.stratio.ioft.domain.{Entry, Field, Value}
import org.json4s._

object ValueSerializer extends CustomSerializer[Value]( format => (
  {  // Extractor
    case jo: JObject =>
      implicit val _ = format
      val value = ((jo \ "value"): @unchecked) match {
        case JInt(v) => v.toInt
        case JDouble(v) => v
        case JDecimal(v) => v.toDouble
        case JString(_) => 0
      }
      Value(
        (jo \ "name").extract[String],
        value
      )
  },
  PartialFunction.empty // Serializer
  )
)

object FieldSerializer extends CustomSerializer[Field]( format => (
  {  // Extractor
    case jo: JObject =>
      implicit val _ = format
      val values = ((jo \ "values"): @unchecked) match {
        case JArray(values: List[JObject @unchecked]) => values.map(_.extract[Value])
      }
      Field(
        (jo \ "name").extract[String],
        (jo \ "type").extract[String],
        (jo \ "unit").extract[String],
        values
      )
  },
  PartialFunction.empty // Serializer
  )
)

object EntrySerializer extends CustomSerializer[Entry]( format => (
  // Extractor
  {
    case jo: JObject =>
      implicit val _ = format

      val fields = ((jo \ "fields"): @unchecked) match {
        case JArray(fields: List[JObject @unchecked]) => fields.map(_.extract[Field])
      }

      val timestamp = ((jo \ "gcs_timestamp_ms"): @unchecked) match {
        case JInt(tsBi) => tsBi
      }

      Entry(
        fields,
        timestamp,
        (jo \ "id").extract[String],
        (jo \ "instance").extract[Long],
        (jo \ "name").extract[String],
        (jo \ "setting").extract[Boolean]
      )
  },
  PartialFunction.empty
  )
)