import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import soccer.SoccerStats

object SoccerMatchAnalyzer {
  private val APP_NAME = "Sports Bets App"

  def load(fileName: String): SoccerStats = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder().appName(APP_NAME).config(conf).getOrCreate()
    val df = sparkSession
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .csv(fileName)
    new SoccerStats(df, sparkSession, sc)
  }

  def main(args: Array[String]): Unit = {
    val stats = load("./data/Soccer/Germany/bundesliga_1998_to_2017.csv")
    val processedStats = stats.process()
  }
}
