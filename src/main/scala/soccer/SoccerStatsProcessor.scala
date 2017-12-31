package soccer

import java.util.UUID

import helpers.DataframeHelper.{udfCorrectJsonString, udfIsJsonString, udfJsonStrToMap}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object SoccerStatsProcessor {
  val EMPTY_JSON_STRING = "{}"

  def castColumns(df: DataFrame): DataFrame = {
    df.withColumn("goals_home", col("goals_home").cast(IntegerType))
      .withColumn("goals_away", col("goals_away").cast(IntegerType))
      .withColumn("date", concat(col("date"), lit(" "), col("time")))
      .withColumn("date", to_timestamp(col("date"), "dd.MM.yyyy HH:mm"))
      .withColumn("players", to_json(struct(col("players"))))
      .withColumn("events", to_json(struct(col("events"))))
      .drop("time")
  }

  def clean(dataFrame: DataFrame): DataFrame = {
    val timestampDf = dataFrame.groupBy("competition_day", "season").agg(first("date") as "competition_date")

    dataFrame.join(timestampDf, Seq("competition_day", "season"))
      .withColumn("date", when(col("date").isNull, col("competition_date")).otherwise(col("date"))).drop("competition_date")
      .withColumn("goals_home", when(col("goals_home").isNull, 0).otherwise(col("goals_home")))
      .withColumn("goals_away", when(col("goals_away").isNull, 0).otherwise(col("goals_away")))
      .withColumn("local", udfCorrectJsonString(col("local")))
      .withColumn("away", udfCorrectJsonString(col("away")))
      .withColumn("local", when(col("local").isNull || !udfIsJsonString(col("local")), EMPTY_JSON_STRING).otherwise(col("local")))
      .withColumn("away", when(col("away").isNull || !udfIsJsonString(col("away")), EMPTY_JSON_STRING).otherwise(col("away")))
      .withColumn("local", udfJsonStrToMap(col("local")))
      .withColumn("away", udfJsonStrToMap(col("away")))
  }

  def preProcess(dataFrame: DataFrame): DataFrame = {
    val generateUUID = udf(() => UUID.randomUUID().toString)

    dataFrame.withColumn("id", generateUUID())
      .withColumn("league", split(col("season"), "-")(0))
      .withColumn("season_start_year", split(col("season"), "-")(1).cast(IntegerType))
      .withColumn("season_end_year", split(col("season"), "-")(2).cast(IntegerType))
      .drop("season")
  }
}
