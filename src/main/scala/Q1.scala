import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions
/**
 * Utility object for Spark initialization and operations related to flight data.
 */
object Q1 {
  /**
   * Retrieves the number of flights for each month from the provided dataset.
   *
   * @param flightDS Dataset of FlightData containing flight information.
   * @return Dataset containing the number of flights for each month, ordered by month.
   */
  def getNumFlightsForEachMonth(flightDS: Dataset[FlightData]) = {
    // Select month from the date column and the flightId
    flightDS.selectExpr("month(date) as month", "flightId")
      // Group by month and count the number of flights
      .groupBy( functions.col("month"))
      .agg(functions.count(functions.col("flightId")).as("Number of Flights"))
      // Order the result by month
      .orderBy( functions.col("month"))
  }
}
