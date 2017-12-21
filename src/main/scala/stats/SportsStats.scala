package stats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SportsStats {
  class SoccerStats(dataFrame: DataFrame){
    private def castColumns(df: DataFrame): DataFrame = {
      df.withColumn("goals_home", col("goals_home").cast(IntegerType))
        .withColumn("goals_away", col("goals_away").cast(IntegerType))
        .withColumn("date", concat(col("date"), lit(" "), col("time")))
        .withColumn("date", to_timestamp(col("date"), "dd.MM.yyyy HH:mm"))
        .drop("time")
    }

    private def clean(dataFrame: DataFrame): DataFrame = {
      val timestampDf = dataFrame.groupBy("competition_day", "season").agg(first("date") as "competition_date")

      dataFrame.join(timestampDf, Seq("competition_day", "season"))
        .withColumn("date", when(col("date").isNull, col("competition_date")).otherwise(col("date"))).drop("competition_date")
        .withColumn("goals_home", when(col("goals_home").isNull, 0).otherwise(col("goals_home")))
        .withColumn("goals_away", when(col("goals_away").isNull, 0).otherwise(col("goals_away")))
    }

    private def preProcess(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("league", split(col("season"), "-")(0))
        .withColumn("season_start_year", split(col("season"), "-")(1).cast(IntegerType))
        .withColumn("season_end_year", split(col("season"), "-")(2).cast(IntegerType))
        .drop("season")
    }

    def process(): DataFrame = {
      val processedData = dataFrame
        .transform(castColumns)
        .transform(clean)
        .transform(preProcess)

      processedData.filter(col("team_home").equalTo("Hertha Berlín")).show(10000)
      processedData.printSchema()

      processedData
    }
  }
}
