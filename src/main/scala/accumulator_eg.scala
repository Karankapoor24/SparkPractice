import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object accumulator_eg extends App {
  val sc = new SparkContext("local[*]", "master")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sample = sc.textFile("C:/Users/karan/OneDrive/Desktop/Codes/Data/samplefile.txt")
  val myAccum = sc.longAccumulator("Blank Lines Accumulator")
  sample.foreach(x => if (x == "") myAccum.add(1))
  println(myAccum.value)
}
