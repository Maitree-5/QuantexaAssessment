import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
/**
 * Object Q2 contains methods related to analyzing frequent flyers using Spark SQL.
 */
object Q2 {
  /**
   * Retrieves the top 100 frequent flyers based on flight counts.
   *
   * @param spark        SparkSession instance for Spark SQL operations.
   * @param flightDataDS Dataset of FlightData containing flight information.
   * @param passengerDS  Dataset of Passenger containing passenger information.
   * @return Dataset containing the top 100 frequent flyers, ordered by flight count.
   *         Each record is a tuple of (passengerId: Int, count: Long, firstName: String, lastName: String).
   */
   def top100FrequentFlyers(
                            spark: SparkSession,
                            flightDataDS: Dataset[FlightData],
                            passengerDS: Dataset[Passenger]
                          ): Dataset[(Int, Long, String, String)] = {

    import spark.implicits._

     // Calculate flight counts per passenger
    val flightCounts = flightDataDS.groupBy("passengerId").count()
     // Join with passenger information, order by flight count descending, limit to top 100
    val frequentFlyers = flightCounts.join(passengerDS, "passengerId") // Join on passengerId
      .orderBy(desc("count")) // Order by flight count in descending order
      .limit(100) // Limit the result to the top 100 frequent flyers
      .select("passengerId", "count", "firstName", "lastName")
      .as[(Int, Long, String, String)] // Cast the result to a Dataset of tuples
     frequentFlyers
  }
}
