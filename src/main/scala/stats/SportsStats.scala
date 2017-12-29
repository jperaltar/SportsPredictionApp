package stats

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import helpers.DataframeHelper._

object SportsStats {
  class SoccerStats(dataFrame: DataFrame, sparkSession: SparkSession, sparkContext: SparkContext){
    val EMPTY_JSON_STRING = "{}"

    private def castColumns(df: DataFrame): DataFrame = {
      df.withColumn("goals_home", col("goals_home").cast(IntegerType))
        .withColumn("goals_away", col("goals_away").cast(IntegerType))
        .withColumn("date", concat(col("date"), lit(" "), col("time")))
        .withColumn("date", to_timestamp(col("date"), "dd.MM.yyyy HH:mm"))
        .withColumn("players", to_json(struct(col("players"))))
        .withColumn("events", to_json(struct(col("events"))))
        .drop("time")
    }

    private def clean(dataFrame: DataFrame): DataFrame = {
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

    private def preProcess(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("league", split(col("season"), "-")(0))
        .withColumn("season_start_year", split(col("season"), "-")(1).cast(IntegerType))
        .withColumn("season_end_year", split(col("season"), "-")(2).cast(IntegerType))
        .drop("season")
    }

    def process(): DataFrame = {
      val processedDF = dataFrame
        .transform(castColumns)
        .transform(clean)
        .transform(preProcess)

      val teamStats = processedDF.select("local")
      val fn = teamStats.schema.fieldNames
      println(fn.toList)
      val processedTeamStats = teamStats.filter(!udfIsEmptyMap(col("local"))).rdd.map { row =>
        row.getAs("local").asInstanceOf[Map[String, String]]
      }

      import sparkSession.implicits._

      val columns = List("ball_control", "shots", "shots_on_goal", "deflected_shots", "shots_out", "offsides",
                        "corners", "saves", "fouls", "yellow_cards", "red_cards")
      val processedTeamStatsDF = processedTeamStats.map{elem =>
        val list = columns.map(key => elem.getOrElse(key, null))
        (list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8), list(9), list(10))
      }.toDF(columns:_*)
      processedTeamStatsDF.show(1000)

      dataFrame
    }
  }
}
