import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum, expr, max}
/**
 * Object Q3 contains methods for analyzing flight data to determine the longest non-UK travel streaks per passenger.
 */
object Q3 {
  /**
   * Determines the longest streak of non-UK flights for each passenger in the dataset.
   *
   * @param flightDataDS Dataset of FlightData containing flight information.
   * @return Dataset containing the longest streak of non-UK flights per passenger, ordered by the length of the streak in descending order.
   */
  def longestNonUKCount(flightDataDS: Dataset[FlightData]) = {
    // Add a new column 'ukArrivals' to indicate if the flight is arriving from the UK
    val ukFlights = flightDataDS.withColumn("ukArrivals",
      sum( expr("case when from = 'uk' then 1 else 0 end"))
        .over( Window.partitionBy(col("passengerId")).orderBy(col("date"))))
    // Group by 'passengerId' and 'ukArrivals' and calculate the non-UK flight count
    ukFlights.groupBy( col("passengerId" ), col("ukArrivals"))
      .agg( (sum( expr(
        """case
          | when 'uk' not in (from,to) then 1
          | when from = to then -1
          | else 0
          |end""".stripMargin)) + 1).as("nonuk"))
      .groupBy("passengerId")
      .agg( max("nonuk").as("LongestRun"))
      .orderBy(col("LongestRun").desc)
  }
}
