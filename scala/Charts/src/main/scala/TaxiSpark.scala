import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.databricks.connect.DatabricksSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.jfree.chart.axis.DateAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.{ChartFactory, ChartFrame, ChartUtils, JFreeChart}
import org.jfree.data.general.DefaultPieDataset
import org.jfree.data.time.{Day, TimeSeries, TimeSeriesCollection}

object TaxiSpark {
  def createSession(): SparkSession = {
    DatabricksSession.builder.remote().getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = createSession()

    trips_per_day(spark)
    local_trips(spark)
  }

  def trips_per_day(spark: SparkSession): Unit = {
    val df = spark.read.table("samples.nyctaxi.trips")

    val trips = df.select("tpep_pickup_datetime")
      .groupBy(to_date(df.col("tpep_pickup_datetime")))
      .count().collect()

    val series = new TimeSeries("trips")
    trips.foreach(row => series.add(new Day(row(0).asInstanceOf[Date]),
      row(1).asInstanceOf[Long]))

    val chart = ChartFactory.createXYLineChart("Number of trips per day",
      "date",
      "trips",
      new TimeSeriesCollection(series))

    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val dateAxis = new DateAxis()
    dateAxis.setDateFormatOverride(new SimpleDateFormat("dd-MM-yyyy"))
    plot.setDomainAxis(dateAxis)

    showChart(chart, Some("pics/trips_per_day.png"))
  }

  def local_trips(spark: SparkSession): Unit = {
    val df = spark.read.table("samples.nyctaxi.trips")

    val trips = df.groupBy(df.col("pickup_zip") === df.col("dropoff_zip")).count().collect()

    val dataset = new DefaultPieDataset[String]()
    trips.foreach(row => dataset.setValue(
      row(0) match {
        case true => "local"
        case _ => "non-local"
      },
      row(1).asInstanceOf[Long]))

    val chart = ChartFactory.createPieChart("Fraction of local trips",
      dataset)

    showChart(chart, Some("pics/localtrips.png"))
  }

  def showChart(chart: JFreeChart, saveTo: Option[String]): Unit = {
    saveTo.foreach(s => ChartUtils.saveChartAsPNG(new File(s), chart, 600, 500))

    val frame = new ChartFrame("Results", chart)
    frame.setSize(600, 500)
    frame.setVisible(true)
  }
}
