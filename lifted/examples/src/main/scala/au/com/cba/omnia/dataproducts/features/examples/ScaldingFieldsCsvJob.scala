package au.com.cba.omnia.dataproducts.features.examples

import com.twitter.scalding._

import cascading.tuple.{ Tuple => CTuple }

class ScaldingFieldsCsvJob(args: Args) extends Job(args) {
  val fields = (1 to 431).map(n => Symbol(s"f$n"))
  val input = Csv(args("input"), fields=fields, skipHeader=true)
  val output = Csv(args("output"), writeHeader=true)
  val error = Csv(args("error"), writeHeader=true)

  input
    .read
    .filter('f2)(predicate)
    .map(('f351, 'f361) -> 'g1)(g1)
    .map(('f356, 'f357) -> 'g2)(g2)
    .map(('f84, 'f83) -> 'g3)(g3)
    .map(('f289, 'f271, 'g1, 'g2, 'g3) -> 'h1)(h1)
    .project(('f103, 'h1))
    .addTrap(error)
    .write(output)

  def round(x: BigDecimal): BigDecimal = x.setScale(2, BigDecimal.RoundingMode.HALF_UP)

  def predicate(v: Int): Boolean = Array(43, 69, 70, 94, 123, 130, 222, 244, 434, 695, 767).contains(v)

  def g1: ((BigDecimal, BigDecimal)) => Int = { case (x, y) =>
    val percentage: BigDecimal = if (y == 0) 0 else round(x / y * 100)

    if (x < 0) {
      if (percentage <= 0) 1
      else if (percentage <= 100) 2
      else 3
    }
    else {
      if (percentage <= 100) 4
      else 5
    }
  }

  def g2: ((Int, Int)) => Int = { case (x, y) =>
    if ( y == 0) 1
    else if (x == 0) 2
    else {
      val percentage = round(BigDecimal(x) / y * 100)
      if (percentage < 26) 3
      else if (percentage < 250) 4
      else 5
    }
  }

  def g3: ((Double, Double)) => Int = { case (x, y) =>
    if (x == 0) 1
    else if (y == 0) 2
    else 3
  }

  def h1: ((Int, Int, Int, Int, Int)) => Int = { case (x, y, g1, g2, g3) =>
    349 +
    (if (x <= 1) 0 else if (x <= 15) -9 else -15) +
    (if (y > 175) 45 else if (y > 96) 35 else if (y > 48) 23 else if (y > 32) 18 else if (y > 9) 13 else 0) +
    (if (g1 == 5) 49 else if (g1 == 3) 36 else 0) +
    (if (g2 == 1 || g2 == 5) 0 else if (g2 == 4) -30 else if (g2 == 3) -63 else if (g2 == 2) -70 else sys.error("Unknown value of g2")) +
    (if (g3 == 1) 0 else if (g3 == 3) -13 else if (g3 == 2) -40 else sys.error("Unknown value of g3"))
  }

  implicit object BigDecimalGetter extends TupleGetter[BigDecimal] {
    override def get(tup: CTuple, i: Int) = BigDecimal(tup.getString(i))
  }
}
