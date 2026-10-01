package earthquakes

import java.util.{Arrays => JArrays, BitSet => JBitSet}

import scala.collection.mutable

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Outcome of the analysis plus per-phase timings (milliseconds). */
final case class Result(
    pair: Long,                 // best pair of cells, see Geo.pair
    count: Int,                 // number of days on which the two cells co-occur
    days: Array[Int],           // those days, ascending (yyyymmdd)
    phases: Seq[(String, Long)] // phase name -> elapsed ms
)

/**
 * Three map-reduce implementations of the same analysis. They all take the
 * events as (cell, day) records and return exactly the same Result: the pair
 * of distinct cells that co-occur on the largest number of days (ties broken
 * by the smallest pair in (lat, lon) lexicographic order), with its days.
 */
object Analysis {

  // ---------------------------------------------------------------- helpers

  private def timed[T](name: String, log: mutable.Buffer[(String, Long)])(body: => T): T = {
    val t0 = System.nanoTime()
    val r = body
    log += name -> (System.nanoTime() - t0) / 1000000L
    r
  }

  /** Associative + commutative "max": more days wins, then smaller pair. */
  private def better(x: (Long, Int), y: (Long, Int)): (Long, Int) =
    if (x._2 > y._2 || (x._2 == y._2 && x._1 < y._1)) x else y

  /** Global maximum in a single job (one local max per partition, then the driver). */
  private def maxOf(counts: RDD[(Long, Int)]): Option[(Long, Int)] =
    counts
      .mapPartitions(it => if (it.hasNext) Iterator(it.reduce(better)) else Iterator.empty)
      .collect()
      .reduceOption(better)

  /** All the pairs a < b of a sorted, duplicate-free array of cells. */
  private def pairsOf(cells: Array[Int]): Iterator[Long] =
    Iterator.range(0, cells.length).flatMap { i =>
      val a = cells(i).toLong << 32
      Iterator.range(i + 1, cells.length).map(j => a | cells(j).toLong)
    }

  private def contains(sorted: Array[Int], c: Int): Boolean = JArrays.binarySearch(sorted, c) >= 0

  /** Days, ascending, on which both cells of `pair` appear. */
  private def daysOf(dayCells: RDD[(Int, Array[Int])], pair: Long): Array[Int] = {
    val (a, b) = (Geo.firstOf(pair), Geo.secondOf(pair))
    dayCells.filter { case (_, cs) => contains(cs, a) && contains(cs, b) }.keys.collect().sorted
  }

  /**
   * Map side of the job: one record per (day, distinct cells of that day).
   * Built with a single shuffle; aggregateByKey de-duplicates the cells
   * already on the map side, so each (cell, day) crosses the network once.
   */
  private def buildDayCells(events: RDD[(Int, Int)], partitions: Int): RDD[(Int, Array[Int])] =
    events
      .map { case (cell, day) => (day, cell) }
      .aggregateByKey(mutable.HashSet.empty[Int], new HashPartitioner(partitions))(
        (set, c) => set += c,
        (s1, s2) => if (s1.size >= s2.size) s1 ++= s2 else s2 ++= s1
      )
      .mapValues(_.toArray.sorted)

  private def noPairs: Nothing =
    throw new IllegalStateException("No two distinct locations ever co-occur on the same day")

  // ------------------------------------------------------ 1) groupByKey

  /**
   * Baseline, the "textbook" formulation:
   *   distinct -> groupByKey(day) -> flatMap(pairs) -> reduceByKey -> max.
   * Two shuffles to obtain the per-day groups (distinct + groupByKey, the
   * latter without map-side combining) and pairs as boxed tuples of tuples.
   */
  def groupByKeyApproach(events: RDD[(Int, Int)], partitions: Int): Result = {
    val log = mutable.Buffer.empty[(String, Long)]

    val dayCells = events
      .distinct(partitions)
      .map { case (cell, day) => (day, cell) }
      .groupByKey(partitions)
      .mapValues(_.toArray.sorted)
      .persist(StorageLevel.MEMORY_AND_DISK)
    timed("day-groups", log)(dayCells.count())

    val (bestPair, bestCount) = timed("pair-count", log) {
      val counts = dayCells
        .flatMap { case (_, cs) =>
          for (i <- cs.indices.iterator; j <- (i + 1 until cs.length).iterator) yield ((cs(i), cs(j)), 1)
        }
        .reduceByKey(_ + _, partitions)
        .map { case ((a, b), n) => (Geo.pair(a, b), n) } // only for the final comparison
      maxOf(counts).getOrElse(noPairs)
    }

    val days = timed("dates", log)(daysOf(dayCells, bestPair))
    dayCells.unpersist(blocking = false)
    Result(bestPair, bestCount, days, log.toList)
  }

  // ------------------------------------------------------ 2) aggregateByKey

  /**
   * Optimised formulation of the same algorithm:
   *  - per-day groups built with one shuffle and map-side de-duplication;
   *  - pairs encoded as a primitive Long, counted with reduceByKey (map-side
   *    combining) on an explicit number of partitions.
   */
  def aggregateApproach(events: RDD[(Int, Int)], partitions: Int): Result = {
    val log = mutable.Buffer.empty[(String, Long)]

    val dayCells = buildDayCells(events, partitions).persist(StorageLevel.MEMORY_AND_DISK)
    timed("day-groups", log)(dayCells.count())

    val best = timed("pair-count", log) {
      val counts = dayCells
        .flatMap { case (_, cs) => pairsOf(cs).map(p => (p, 1)) }
        .reduceByKey(_ + _, partitions)
      maxOf(counts).getOrElse(noPairs)
    }

    val days = timed("dates", log)(daysOf(dayCells, best._1))
    dayCells.unpersist(blocking = false)
    Result(best._1, best._2, days, log.toList)
  }

  // ------------------------------------------------------ 3) pruning

  /**
   * Exact algorithm that avoids generating most of the pairs.
   *
   * Two cells a, b cannot co-occur on more days than min(days(a), days(b)).
   * Hence, if some pair is known to co-occur L times, the optimal pair (and
   * every pair tied with it) is made only of cells active on >= L days.
   *
   *  1. days(c) for every cell (a cheap word-count-like reduceByKey);
   *  2. lower bound L: exact best pair among the `topK` most active cells;
   *  3. candidates = cells with days(c) >= L. If they are all among the top-K
   *     cells, step 2 already considered every candidate pair and its result
   *     is the optimum; otherwise the pairs of the candidates are counted.
   *
   * The broadcast sets are bitsets over the cell ids (~800 KB).
   */
  def pruningApproach(events: RDD[(Int, Int)], partitions: Int, topK: Int): Result = {
    val log = mutable.Buffer.empty[(String, Long)]
    val sc = events.sparkContext

    val dayCells = buildDayCells(events, partitions).persist(StorageLevel.MEMORY_AND_DISK)
    timed("day-groups", log)(dayCells.count())

    def bestAmong(cells: Iterable[Int]): Option[(Long, Int)] = {
      val bits = new JBitSet()
      cells.foreach(c => bits.set(c))
      val bc = sc.broadcast(bits)
      val counts = dayCells
        .flatMap { case (_, cs) => pairsOf(cs.filter(c => bc.value.get(c))).map(p => (p, 1)) }
        .reduceByKey(_ + _, partitions)
      val best = maxOf(counts)
      bc.destroy()
      best
    }

    val activity = dayCells
      .flatMap { case (_, cs) => cs.iterator.map(c => (c, 1)) }
      .reduceByKey(_ + _, partitions)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val nCells = timed("cell-activity", log)(activity.count())

    val (top, lowerBound) = timed("lower-bound", log) {
      val top = activity.top(topK)(Ordering.by { case (c, n) => (n, -c) }).map(_._1)
      (top, bestAmong(top))
    }

    val best = timed("pair-count", log) {
      val threshold = lowerBound.map(_._2).getOrElse(1)
      val candidates = activity.filter(_._2 >= threshold).keys.collect()
      val topSet = top.toSet
      val fromTop = lowerBound.isDefined && candidates.forall(topSet.contains)
      println(s"PRUNING topK=${top.length} lowerBound=$threshold candidates=${candidates.length} " +
        s"cells=$nCells exactFromTopK=$fromTop")
      val exact = if (fromTop) lowerBound else bestAmong(candidates)
      exact.getOrElse(noPairs)
    }

    val days = timed("dates", log)(daysOf(dayCells, best._1))
    activity.unpersist(blocking = false)
    dayCells.unpersist(blocking = false)
    Result(best._1, best._2, days, log.toList)
  }
}
