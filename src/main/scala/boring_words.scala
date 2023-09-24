import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import scala.io.Source

object boring_words extends App{

  // BroadCast Variable Example
  def loadBoringWords(): Set[String] = {
    var boringWords: Set[String] = Set()
    val lines = Source.fromFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/boringwords.txt").getLines()
    for(line <- lines) {
      boringWords += line
    }
    boringWords
  }
  val sc = new SparkContext("local[*]","master")
  Logger.getLogger("org").setLevel(Level.ERROR)
  var nameSet = sc.broadcast(loadBoringWords)
  val rdd1 = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/bigdata-campaign.csv")
  val rdd2 = rdd1.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
  val rdd3 = rdd2.flatMapValues(x => x.split(" "))
  val rdd4 = rdd3.map(x => (x._2.toLowerCase(),x._1))
  val filteredRDD = rdd4.filter(x => !nameSet.value(x._1))
  val rdd5 = filteredRDD.reduceByKey((x,y) => x+y)
  val rdd6 = rdd5.sortBy(x => x._2, false)
  //val result = rdd6.collect().foreach(println)
  rdd6.take(20).foreach(println)


}

