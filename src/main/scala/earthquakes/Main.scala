package earthquakes

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Earthquake co-occurrence analysis.
 *
 * Usage (arguments are optional, defaults in brackets):
 *   --input <path>       CSV with header longitude,latitude,date     [required]
 *   --output <path>      directory for the result (overwritten)      [none: stdout only]
 *   --approach <name>    groupbykey | aggregate | pruning             [pruning]
 *   --partitions <n>     partitions for repartition and shuffles      [spark.default.parallelism]
 *   --topk <k>           cells used for the lower bound (pruning)     [64]
 *   --rounding <mode>    half-up | half-even | math-round (ties only) [half-up]
 *
 * Output: first line the pair "((lat1, lon1), (lat2, lon2))", then the days
 * on which the two locations co-occur, in ascending order.
 * A line starting with "METRICS " reports timings in JSON (used by the benchmark scripts).
 */
object Main {

  final case class Config(
      input: String = "",
      output: Option[String] = None,
      approach: String = "pruning",
      partitions: Int = 0,
      topK: Int = 64,
      rounding: String = "half-up"
  )

  private def parseArgs(args: Array[String]): Config = {
    def loop(rest: List[String], c: Config): Config = rest match {
      case Nil                          => c
      case "--input" :: v :: tail       => loop(tail, c.copy(input = v))
      case "--output" :: v :: tail      => loop(tail, c.copy(output = Some(v)))
      case "--approach" :: v :: tail    => loop(tail, c.copy(approach = v.toLowerCase))
      case "--partitions" :: v :: tail  => loop(tail, c.copy(partitions = v.toInt))
      case "--topk" :: v :: tail        => loop(tail, c.copy(topK = v.toInt))
      case "--rounding" :: v :: tail    => loop(tail, c.copy(rounding = v.toLowerCase))
      case other :: _                   => sys.error(s"Unknown or incomplete argument: $other")
    }
    val c = loop(args.toList, Config())
    require(c.input.nonEmpty, "--input is required")
    require(Set("groupbykey", "aggregate", "pruning")(c.approach), s"unknown approach ${c.approach}")
    require(Geo.RoundingModes(c.rounding), s"unknown rounding mode ${c.rounding}")
    require(c.partitions >= 0 && c.topK >= 2, "--partitions must be >= 0 and --topk >= 2")
    c
  }

  /** CSV rows -> (cell, day). Column positions are taken from the header. */
  private def loadEvents(spark: SparkSession, path: String, rounding: String): RDD[(Int, Int)] = {
    val df = spark.read.option("header", value = true).csv(path)
    val cols = df.columns.map(_.trim.toLowerCase)
    val (iLon, iLat, iDate) = (cols.indexOf("longitude"), cols.indexOf("latitude"), cols.indexOf("date"))
    require(iLon >= 0 && iLat >= 0 && iDate >= 0, s"unexpected header: ${df.columns.mkString(",")}")

    df.rdd.map { row =>
      val lat = Geo.toTenths(row.getString(iLat), rounding)
      val lon = Geo.toTenths(row.getString(iLon), rounding)
      require(math.abs(lat) <= 900 && math.abs(lon) <= 1800, s"coordinates out of range: $row")
      (Geo.cell(lat, lon), Geo.day(row.getString(iDate)))
    }
  }

  def main(args: Array[String]): Unit = {
    val cfg = parseArgs(args)

    val conf = new SparkConf()
      .setAppName(s"earthquake-cooccurrence-${cfg.approach}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]") // plain `sbt run`
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    try {
      val partitions = if (cfg.partitions > 0) cfg.partitions else sc.defaultParallelism
      val t0 = System.nanoTime()

      val parsed = loadEvents(spark, cfg.input, cfg.rounding)
      val events = if (cfg.partitions > 0) parsed.repartition(partitions) else parsed

      val result = cfg.approach match {
        case "groupbykey" => Analysis.groupByKeyApproach(events, partitions)
        case "aggregate"  => Analysis.aggregateApproach(events, partitions)
        case "pruning"    => Analysis.pruningApproach(events, partitions, cfg.topK)
      }

      val lines = Geo.formatPair(result.pair) +: result.days.map(Geo.formatDay)
      cfg.output.foreach { out =>
        val path = new Path(out)
        val fs = path.getFileSystem(sc.hadoopConfiguration)
        if (fs.exists(path)) fs.delete(path, true)
        sc.parallelize(lines.toSeq, numSlices = 1).saveAsTextFile(out) // a single part-00000 file
      }
      val seconds = (System.nanoTime() - t0) / 1e9

      // ---- report
      println(lines.head)
      if (lines.length <= 21) lines.tail.foreach(println)
      else {
        lines.slice(1, 11).foreach(println)
        println(s"... (${lines.length - 21} more dates) ...")
        lines.takeRight(10).foreach(println)
      }
      val executors = sc.statusTracker.getExecutorInfos.length - 1 // minus the driver
      val phases = result.phases.map { case (k, v) => s""""$k":$v""" }.mkString(",")
      println(
        s"""METRICS {"approach":"${cfg.approach}","rounding":"${cfg.rounding}","partitions":$partitions,""" +
          s""""inputPartitions":${parsed.getNumPartitions},"defaultParallelism":${sc.defaultParallelism},""" +
          s""""executors":${math.max(executors, 1)},"pair":"${Geo.formatPair(result.pair)}",""" +
          s""""cooccurrences":${result.count},"seconds":${"%.3f".formatLocal(java.util.Locale.ROOT, seconds)},"phasesMs":{$phases}}"""
      )
    } finally spark.stop()
  }
}
