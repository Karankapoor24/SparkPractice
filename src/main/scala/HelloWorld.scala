import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("My First Scala Program")
    Logger.getLogger("org").setLevel(Level.OFF)
    // For Windows
    //System.setProperty("hadoop.home.dir","C:\\hadoop")
    //val sc = new SparkContext("local[*]", "wordcount")
    //val input = sc.textFile("C:\\Users\\karan\\OneDrive\\Desktop\\Codes\\Spark\\search_data.txt")
    //input.collect.foreach(println)
    val spark = SparkSession.builder().appName("Hello")
                .config("spark.master","local")
                .enableHiveSupport()
                .getOrCreate()

    println("Spark Session Created")

    val sampleSeq = Seq((1,"Spark"),(2,"Big Data"))
    val df = spark.createDataFrame(sampleSeq).toDF(colNames = "Course id", "Course Name")
    df.show()
    df.write.format("csv").save("first_df")
  }
}
