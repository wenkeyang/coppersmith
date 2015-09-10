package commbank.coppersmith.scalding.lift

import au.com.cba.omnia.thermometer.core.ThermometerSpec
import com.twitter.scalding.typed._
import commbank.coppersmith._
import commbank.coppersmith.scalding.lift.scalding._
import commbank.coppersmith.test.thrift._

import ScaldingScalazInstances._

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
