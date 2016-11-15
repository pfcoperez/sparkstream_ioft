package com.stratio.ioft.domain



  case class Value(name: String, value: AnyVal)

  case class Field(name: String, tpe: String, unit: String, values: List[Value])

  case class Entry(
                    fields: List[Field],
                    gcs_timestamp_ms: BigInt,
                    id: String,
                    instance: Long,
                    name: String,
                    setting: Boolean
                  )


