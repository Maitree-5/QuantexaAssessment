import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, count, max, min}

object BonusQ {
  /**
   * Finds pairs of passengers who flew together on more than N flights within a specified date range.
   *
   * @param flightDS Dataset of FlightData containing flight information.
   * @param atLeastNTimes Minimum number of flights together.
   * @param from Start date of the date range in "yyyy-MM-dd" format.
   * @param to End date of the date range in "yyyy-MM-dd" format.
   * @return Dataset containing pairs of passengers who flew together on more than N flights,
   *         along with the number of flights together and the date range of their flights.
   */
  def flownTogether(atLeastNTimes: Int, from: String, to: String, flightDS: Dataset[FlightData]) = {
    // Self-join the dataset to find pairs of passengers who have been on the same flights on the same dates
    flightDS.filter(col("date").between(from, to))
      .as("t1").join(
      flightDS.filter(col("date").between(from, to)).as("t2"),
      col("t1.passengerId") < col("t2.passengerId") &&
        col("t1.flightId") === col("t2.flightId") &&
        col("t1.date") === col("t2.date"),
      "inner"
    ).groupBy(col("t1.passengerId"), col("t2.passengerId"))
      .agg(
        count("*").as("numFlightsTogether"),
        min("t1.date").as("from"),
        max("t1.date").as("to")
      )
      .where(col("numFlightsTogether") > atLeastNTimes)
      .orderBy(col("numFlightsTogether").desc)
      .selectExpr(
        "t1.passengerId as passenger1",
        "t2.passengerId as passenger2",
        "numFlightsTogether",
        "from",
        "to"
      )
  }
}
