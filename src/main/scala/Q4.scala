import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, count, max, min}
/**
 * Object Q4 contains methods for finding pairs of passengers who flew together on multiple flights.
 */
object Q4 {
  /**
   * Finds pairs of passengers who flew together on at least 3 flights, along with the date range of their flights.
   *
   * @param flightDS Dataset of FlightData containing flight information.
   * @return Dataset containing pairs of passengers who flew together on at least 3 flights,
   *         along with the number of flights together and the date range of their flights.
   */
  def findFlightsTogether( flightDS: Dataset[FlightData]) = {
    // Self-join the dataset to find pairs of passengers who have been on the same flights on the same dates
    flightDS.as("t1").join(
      flightDS.as("t2"),
      col("t1.passengerId") < col("t2.passengerId") &&
        col("t1.flightId") === col("t2.flightId") &&
        col("t1.date") === col("t2.date"),
      "inner"
    ).groupBy( col("t1.passengerId"), col("t2.passengerId"))
      .agg( count("*").as("numFlightsTogether"),
        min("t1.date").as("from"),
        max("t1.date").as("to"))
      .where("numFlightsTogether >= 3")
      .orderBy(col("numFlightsTogether").desc)
      .selectExpr("t1.passengerId as passenger1",
        "t2.passengerId as passenger2",
        "numFlightsTogether",
        "from",
        "to")

  }
}
