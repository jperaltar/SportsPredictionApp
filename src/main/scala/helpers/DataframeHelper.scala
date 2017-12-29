package helpers

import org.apache.spark.sql.functions.udf
import org.json4s.jackson.JsonMethods.parse

object DataframeHelper {
  private val isJsonString: (String => Boolean) = (string: String) => {
    if (!string.startsWith("{") || !string.endsWith("}")) {
      false
    } else {
      try {
        parse(string)
        true
      } catch {
        case e: ClassCastException => false
        case e: Exception => false
      }
    }
  }

  private val correctJsonString: (String => String) = (jsonStr: String) => {
    val initialQuotePattern = "(?<=[\\s{:,]+)'"
    val finalQuotePattern = "'(?=[\\s}:,]+)"
    val uQuotePattern = "u'(?![\\s}:,])"

    if (jsonStr != null) {
      jsonStr.replaceAll(initialQuotePattern , "\"")
        .replaceAll(finalQuotePattern, "\"")
        .replaceAll(uQuotePattern, "\"")
    } else jsonStr

  }

  private val jsonStrToMap: (String => Map[String, String]) = (jsonStr: String) => {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, String]]
  }

  private val isEmptyMap: (Map[String, Any] => Boolean) = (map: Map[String, Any]) => {
    if (map.keys.size == 0) true else false
  }

  val udfIsJsonString = udf(isJsonString)
  val udfCorrectJsonString = udf(correctJsonString)
  val udfJsonStrToMap = udf(jsonStrToMap)
  val udfIsEmptyMap = udf(isEmptyMap)
}
