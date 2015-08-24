package au.com.cba.omnia.dataproducts.features.lift

import java.util.concurrent.Executors

import au.com.cba.omnia.dataproducts.features.Join
import au.com.cba.omnia.dataproducts.features.lift.scalding._
import au.com.cba.omnia.dataproducts.features.test.thrift._
import au.com.cba.omnia.thermometer.tools.Executions
import com.twitter.scalding.{Args, Hdfs, Mode, Config}
import com.twitter.scalding.typed.IterablePipe

import au.com.cba.omnia.thermometer.core._


class ScaldingSpec extends ThermometerSpec {
  def is = s2"""
     inner join $joins
     left join  $leftJoins
  """


  val cs = List(
    C(1, "1"),
    C(2, "2"),
    C(2, "22"),
    C(3, "333")
  )
  val ds = List(
    D(2, "2"),
    D(2, "22"),
    D(2, "222"),
    D(3, "333"),
    D(4, "4")
  )

  def joins = {
    val joinedPipe = liftJoin(Join[C].to[D].on(_.id, _.id))(IterablePipe(cs), IterablePipe(ds))
    val expected = List(
      C(2,"2") -> D(2, "2"),
      C(2,"2") -> D(2, "22"),
      C(2,"2") -> D(2, "222"),
      C(2,"22") -> D(2, "2"),
      C(2,"22") -> D(2, "22"),
      C(2,"22") -> D(2, "222"),
      C(3,"333") -> D(3, "333")
    )
    runsSuccessfully(joinedPipe) must_== expected
  }

  def leftJoins = {
    val expected = List(
      C(1, "1") -> None,
      C(2,"2") -> Some(D(2, "2")),
      C(2,"2") -> Some(D(2, "22")),
      C(2,"2") -> Some(D(2, "222")),
      C(2,"22") -> Some(D(2, "2")),
      C(2,"22") -> Some(D(2, "22")),
      C(2,"22") -> Some(D(2, "222")),
      C(3,"333") -> Some(D(3, "333"))
    )

    val joinedPipe = liftLeftJoin(Join.left[C].to[D].on(_.id, _.id))(IterablePipe(cs), IterablePipe(ds))
    runsSuccessfully(joinedPipe) must_== expected
  }
}
