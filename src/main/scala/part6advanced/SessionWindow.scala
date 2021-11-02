package part6advanced

import java.io.PrintStream
import java.net.ServerSocket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

object SessionWindow {

  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // 3000,blue

  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testSessionWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .selectExpr("from_csv(value, 'created LONG, color STRING') as color")
      .selectExpr("color.color","timestamp_millis(color.created) as created")


    val watermarkedDF = dataDF
      .withWatermark("created", "1 seconds") // adding a 2 second watermark
      .groupBy(col("color"), session_window(col("created"), "5 seconds"))
      .count()
      .selectExpr("session_window.*", "color", "count")

    /*
      A 2 second watermark means
      - a window will only be considered until the watermark surpasses the window end
      - an element/a row/a record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start()

    debugQuery(query)
    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    testSessionWatermark()
  }
}

// sending data "manually" through socket to be as deterministic as possible
object DataSenderSessionWindow {
  val serverSocket = new ServerSocket(9999)
  println("socket waiting")
  val socket = serverSocket.accept() // blocking call
  val printer = new PrintStream(socket.getOutputStream)

  println("socket accepted")

  def example1() = {
    Thread.sleep(5000)
    printer.println("5000,blue")
    Thread.sleep(1000)
    printer.println("6000,blue")
    Thread.sleep(1000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,blue")
    Thread.sleep(1000)
    printer.println("9000,blue")
    Thread.sleep(6000)
    printer.println("15000,red")
    Thread.sleep(1000)
    printer.println("16000,blue")
    Thread.sleep(10000)
    printer.println("30000,blue")

  }

  def main(args: Array[String]): Unit = {
    example1()
  }
}

