package stats

import org.apache.spark.sql.DataFrame

trait SportStats {
  var dataFrame: DataFrame
  def getGeneralInfo: DataFrame
  def getIndividualStats: DataFrame
  def getEventStats: DataFrame
}
