package au.com.cba.omnia.dataproducts.features.examples

import scala.collection.immutable.BitSet

import com.twitter.scalding._

import au.com.cba.omnia.maestro.api.Macros


import au.com.cba.omnia.dataproducts.features.examples.thrift.Wide1

class ScaldingTypedCsvJob(args: Args) extends Job(args) {
  val input = TextLine(args("input"))
  val output = TypedCsv[(String, Int)](args("output"))

  Util.decodeHive(input, sep=",")(Macros.mkDecode[Wide1])
    ._1
    .filter { case wide1               => predicate(wide1.f2) }
    .map    { case wide1               => (wide1, g1(BigDecimal(wide1.f351), BigDecimal(wide1.f361))) }
    .map    { case (wide1, g1)         => (wide1, g1, g2(wide1.f356, wide1.f357)) }
    .map    { case (wide1, g1, g2)     => (wide1, g1, g2, g3(wide1.f84, wide1.f83)) }
    .map    { case (wide1, g1, g2, g3) => (wide1.f103, h1(wide1.f289, wide1.f271, g1, g2, g3)) }
    .write(output)

  def round(x: BigDecimal): BigDecimal = x.setScale(2, BigDecimal.RoundingMode.HALF_UP)

  val predicate: Int => Boolean = BitSet(43, 69, 70, 94, 123, 130, 222, 244, 434, 695, 767).contains

  def g1(x: BigDecimal, y: BigDecimal): Int = {
    val percentage: BigDecimal = if (y == 0) 0 else round(x / y * 100)

    if (x < 0) {
      if (percentage <= 0) 1
      else if (percentage <= 100) 2
      else 3
    } else {
      if (percentage <= 100) 4
      else 5
    }
  }

  def g2(x: Int, y: Int): Int = {
    if (y == 0) 1
    else if (x == 0) 2
    else {
      val percentage = round(BigDecimal(x) / y * 100)
      if (percentage < 26) 3
      else if (percentage < 250) 4
      else 5
    }
  }

  def g3(x: Double, y: Double): Int = {
    if (x == 0) 1
    else if (y == 0) 2
    else 3
  }

  def h1(x: Int, y: Int, g1: Int, g2: Int, g3: Int): Int = {
    349 +
    (if (x <= 1) 0 else if (x <= 15) -9 else -15) +
    (if (y > 175) 45 else if (y > 96) 35 else if (y > 48) 23 else if (y > 32) 18 else if (y > 9) 13 else 0) +
    (if (g1 == 5) 49 else if (g1 == 3) 36 else 0) +
    (if (g2 == 1 || g2 == 5) 0 else if (g2 == 4) -30 else if (g2 == 3) -63 else if (g2 == 2) -70 else sys.error("Unknown value of g2")) +
    (if (g3 == 1) 0 else if (g3 == 3) -13 else if (g3 == 2) -40 else sys.error("Unknown value of g3"))
  }
}
