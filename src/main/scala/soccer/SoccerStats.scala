package soccer

import helpers.DataframeHelper.udfIsEmptyMap
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
    val columns = List("ball_control", "shots", "shots_on_goal", "deflected_shots", "shots_out", "offsides",
      "corners", "saves", "fouls", "yellow_cards", "red_cards")
    val teamStats = dataFrame.select("local")
    val fn = teamStats.schema.fieldNames
    println(fn.toList)
    val processedTeamStats = teamStats.filter(!udfIsEmptyMap(col("local"))).rdd.map { row =>
      row.getAs("local").asInstanceOf[Map[String, String]]
    }

    import sparkSession.implicits._
    val processedTeamStatsDF = processedTeamStats.map{elem =>
      val list = columns.map(key => elem.getOrElse(key, null))
      (list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8), list(9), list(10))
    }.toDF(columns:_*)
    processedTeamStatsDF.show(1000)
    processedTeamStatsDF
  }

  override def getEventStats: DataFrame = {
    dataFrame
  }

  def process(): Map[String, DataFrame] = {
    val teamStatsDF = getTeamStats
    teamStatsDF.show()

    Map("team_stats" -> teamStatsDF)
  }
}
