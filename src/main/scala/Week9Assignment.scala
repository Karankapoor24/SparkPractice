import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import scala.math.min

object Week9Assignment extends App {
  val sc = new SparkContext("local[*]", "master")
  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line: String)= {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3)
    (stationId,entryType,temperature)
  }

  /*
  // A1 Solution
  val rdd1 = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/W9A1.csv")
  //rdd1.collect().foreach(println)
  val rdd2 = rdd1.map(line => {
    val fields = line.split(",")
    if (fields(1).toInt > 18)
      (fields(0), fields(1), fields(2), "Y")
    else
      (fields(0), fields(1), fields(2), "N")
  }
  )
  rdd2.collect().foreach(println)
   */

  val lines = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/W9A2.csv")
  val parsedLines = lines.map(parseLine)
  val minTemps = parsedLines.filter(x => x._2 == "TMIN")
  val stationTemps = minTemps.map(x => (x._1,x._3.toFloat))
  val minTempsByStation = stationTemps.reduceByKey((x,y) => min(x,y))
  val results = minTempsByStation.collect()

  for(result <- results.sorted) {
    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f F"
    println(s"$station minimum temprature : $formattedTemp")
  }

}
