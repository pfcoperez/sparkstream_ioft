package com.stratio.ioft.serialization.json4s

import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{FlatSpec, Matchers}
import org.json4s.jackson.JsonMethods._

class LibrePilotSerializationSpec extends FlatSpec with Matchers {

  implicit val _: Formats = DefaultFormats + FieldSerializer

  "json4s" should "be able to deserialize LibrePilot's values" in {

    val valueJson =
      """
        |{
        |  "name": "0",
        |  "value": 0.65884
        |}
      """.stripMargin

    parse(valueJson).extract[Value] shouldBe Value("0", 0.65884)

  }

  "json4s" should "be able to deserialize LibrePilot's fields" in {

    val valueJson =
      """
        |{
        |      "name": "Yaw",
        |      "type": "float32",
        |      "unit": "degrees",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": -97.40
        |        }
        |      ]
        |}
      """.stripMargin

    parse(valueJson).extract[Field] shouldBe Field(
      "Yaw",
      "float32",
      "degrees",
      Value("0", -97.40) :: Nil
    )

  }

  "json4s" should "be able to deserialize LibrePilot's whole entries" in {

    val valueJson =
      """
        |{
        |  "fields": [
        |    {
        |      "name": "q1",
        |      "type": "float32",
        |      "unit": "",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": 0.6588495373725891
        |        }
        |      ]
        |    },
        |    {
        |      "name": "q2",
        |      "type": "float32",
        |      "unit": "",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": 0.004345187917351723
        |        }
        |      ]
        |    },
        |    {
        |      "name": "q3",
        |      "type": "float32",
        |      "unit": "",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": -0.0016269430052489042
        |        }
        |      ]
        |    },
        |    {
        |      "name": "q4",
        |      "type": "float32",
        |      "unit": "",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": -0.7500156760215759
        |        }
        |      ]
        |    },
        |    {
        |      "name": "Roll",
        |      "type": "float32",
        |      "unit": "degrees",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": 0.4694768786430359
        |        }
        |      ]
        |    },
        |    {
        |      "name": "Pitch",
        |      "type": "float32",
        |      "unit": "degrees",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": 0.2506181001663208
        |        }
        |      ]
        |    },
        |    {
        |      "name": "Yaw",
        |      "type": "float32",
        |      "unit": "degrees",
        |      "values": [
        |        {
        |          "name": "0",
        |          "value": -97.40373992919922
        |        }
        |      ]
        |    }
        |  ],
        |  "gcs_timestamp_ms": 1459627597025,
        |  "id": "D7E0D964",
        |  "instance": 0,
        |  "name": "AttitudeState",
        |  "setting": false
        |}
        |
      """.stripMargin

    parse(valueJson).extract[Entry] shouldBe Entry(
      List(
        Field("q1", "float32", "", Value("0", 0.6588495373725891) :: Nil),
        Field("q2", "float32", "", Value("0", 0.004345187917351723) :: Nil),
        Field("q3", "float32", "", Value("0", -0.0016269430052489042) :: Nil),
        Field("q4", "float32", "", Value("0", -0.7500156760215759) :: Nil),
        Field("Roll", "float32", "degrees", Value("0", 0.4694768786430359) :: Nil),
        Field("Pitch", "float32", "degrees", Value("0", 0.2506181001663208) :: Nil),
        Field("Yaw", "float32", "degrees", Value("0", -97.40373992919922) :: Nil)
      ),
      BigInt("1459627597025"),
      "D7E0D964",
      0,
      "AttitudeState",
      false
    )


  }

}
