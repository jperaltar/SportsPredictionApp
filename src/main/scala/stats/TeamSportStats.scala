package stats

import org.apache.spark.sql.DataFrame

trait TeamSportStats extends SportStats{
  def getTeamStats: DataFrame
}
