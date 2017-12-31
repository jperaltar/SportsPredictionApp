package soccer

import helpers.DataframeHelper.udfIsEmptyMap
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import stats.TeamSportStats

class SoccerStats(var dataFrame: DataFrame, sparkSession: SparkSession, sparkContext: SparkContext) extends TeamSportStats{
  dataFrame = dataFrame.transform(SoccerStatsProcessor.castColumns)
    .transform(SoccerStatsProcessor.clean)
    .transform(SoccerStatsProcessor.preProcess)

  override def getGeneralInfo: DataFrame = {
    dataFrame
  }

  override def getIndividualStats: DataFrame = {
    dataFrame
  }

  override def getTeamStats: DataFrame = {
    def clean(dataFrame: DataFrame): DataFrame = {
      dataFrame.withColumn("red_cards", when(col("red_cards").isNull, 0).otherwise(col("red_cards")))
        .withColumn("yellow_cards", when(col("yellow_cards").isNull, 0).otherwise(col("yellow_cards")))
        .withColumn("shots_on_goal", when(col("shots").isNotNull && col("shots_on_goal").isNull, 0).otherwise(col("shots_on_goal")))
        .withColumn("deflected_shots", when(col("shots").isNotNull && col("deflected_shots").isNull, 0).otherwise(col("deflected_shots")))
        .withColumn("shots_out", when(col("shots").isNotNull && col("shots_out").isNull, 0).otherwise(col("shots_out")))
    }

    val columns = List("match_id", "team", "ball_control", "shots", "shots_on_goal", "deflected_shots", "shots_out",
      "offsides", "corners", "saves", "fouls", "yellow_cards", "red_cards")
    val teamStats = dataFrame.select("id", "team_home", "team_away", "local", "away")
    val processedTeamStats = teamStats.filter(!udfIsEmptyMap(col("local"))).rdd.flatMap { row =>
      val localStatsMap = row.getAs("local").asInstanceOf[Map[String, String]]
      val awayStatsMap = row.getAs("away").asInstanceOf[Map[String, String]]
      val localInfoMap = Map("match_id" -> row.getAs("id"), "team" -> row.getAs("team_home"))
      val awayInfoMap = Map("match_id" -> row.getAs("id"), "team" -> row.getAs("team_away"))
      Seq(localInfoMap ++ localStatsMap, awayInfoMap ++ awayStatsMap)
    }

    import sparkSession.implicits._
    val processedTeamStatsDF = processedTeamStats.map{elem =>
      val list = columns.map(key => elem.getOrElse(key, null))
      (list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8), list(9), list(10), list(11), list(12))
    }.toDF(columns:_*).transform(clean)
    processedTeamStatsDF.show(1000)
    processedTeamStatsDF
  }

  override def getEventStats: DataFrame = {
    dataFrame
  }

  def process(): Map[String, DataFrame] = {
    dataFrame.show()
    val teamStatsDF = getTeamStats

    Map("team_stats" -> teamStatsDF)
  }
}
