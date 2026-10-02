package earthquakes

import java.math.{BigDecimal => JBigDecimal, RoundingMode}

/**
 * Compact encodings used throughout the job.
 *
 *  - Coordinates are rounded to the first decimal digit and kept as integer
 *    "tenths of degree" (e.g. 37.502 -> 375, -122.85 -> -1229).
 *  - A cell (lat, lon) is packed in a single Int, ordered lexicographically
 *    by (latitude, longitude): comparing two cell ids == comparing the pairs.
 *  - A pair of cells (a < b) is packed in a single Long.
 *  - A day is the Int yyyymmdd, whose natural order is the chronological one.
 *
 * Working with primitives instead of tuples/strings reduces the size of the
 * shuffled records and the pressure on the garbage collector.
 */
object Geo {

  private val LatOffset = 900   // latitude  in [-90.0, 90.0]   -> [0, 1800]
  private val LonOffset = 1800  // longitude in [-180.0, 180.0] -> [0, 3600]
  private val LonRange  = 3601

  /** How a value exactly half-way between two tenths (e.g. -122.85) is rounded. */
  val RoundingModes: Set[String] = Set("half-up", "half-even", "math-round")

  /**
   * Rounds a decimal string to the nearest tenth and returns it as tenths
   * (e.g. "37.502" -> 375). Only exact ties depend on `mode`:
   *
   *  - "half-up" (default): away from zero, the usual "school" rounding and
   *    BigDecimal's HALF_UP:              11.25 -> 11.3   -122.85 -> -122.9
   *  - "half-even": banker's rounding:   11.25 -> 11.2   -122.85 -> -122.8
   *  - "math-round": towards +infinity, i.e. what math.round(x * 10) does:
   *                                       11.25 -> 11.3   -122.85 -> -122.8
   *
   * The computation uses exact decimal arithmetic on the CSV string, so it
   * never depends on the binary representation of a Double.
   */
  def toTenths(s: String, mode: String = "half-up"): Int = {
    val x = new JBigDecimal(s.trim)
    val rm = mode match {
      case "half-up"    => RoundingMode.HALF_UP
      case "half-even"  => RoundingMode.HALF_EVEN
      case "math-round" => if (x.signum < 0) RoundingMode.HALF_DOWN else RoundingMode.HALF_UP
    }
    x.setScale(1, rm).unscaledValue.intValueExact
  }

  def cell(latTenths: Int, lonTenths: Int): Int =
    (latTenths + LatOffset) * LonRange + (lonTenths + LonOffset)

  def latOf(cell: Int): Int = cell / LonRange - LatOffset
  def lonOf(cell: Int): Int = cell % LonRange - LonOffset

  /** Pair key with the smaller cell in the high 32 bits (cells are >= 0). */
  def pair(a: Int, b: Int): Long =
    if (a < b) (a.toLong << 32) | b.toLong else (b.toLong << 32) | a.toLong

  def firstOf(pair: Long): Int  = (pair >>> 32).toInt
  def secondOf(pair: Long): Int = (pair & 0xffffffffL).toInt

  /** "2024-03-12 02:10:00+00:00" -> 20240312 (the timestamps are in UTC). */
  def day(timestamp: String): Int = {
    val t = timestamp.trim
    require(t.length >= 10 && t.charAt(4) == '-' && t.charAt(7) == '-', s"bad date: $timestamp")
    t.substring(0, 4).toInt * 10000 + t.substring(5, 7).toInt * 100 + t.substring(8, 10).toInt
  }

  // ---- formatting -------------------------------------------------------

  private def tenthsToString(t: Int): String = JBigDecimal.valueOf(t.toLong, 1).toPlainString

  /** Cell -> "(lat, lon)", e.g. "(37.5, 15.3)". */
  def formatCell(cell: Int): String =
    s"(${tenthsToString(latOf(cell))}, ${tenthsToString(lonOf(cell))})"

  /** Pair -> "((lat1, lon1), (lat2, lon2))". */
  def formatPair(pair: Long): String =
    s"(${formatCell(firstOf(pair))}, ${formatCell(secondOf(pair))})"

  def formatDay(day: Int): String = f"${day / 10000}%04d-${day / 100 % 100}%02d-${day % 100}%02d"
}
