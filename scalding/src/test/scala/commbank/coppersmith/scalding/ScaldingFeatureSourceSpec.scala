package commbank.coppersmith.scalding

import com.twitter.scalding.typed._

import org.scalacheck.Prop.forAll

import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import au.com.cba.omnia.thermometer.core.ThermometerSpec

import commbank.coppersmith._, SourceBinder._
import Arbitraries._
import test.thrift.{Account, Customer}

object ScaldingFeatureSourceSpec extends ThermometerSpec { def is = s2"""
  SelectFeatureSet - Test an example set of features based on selecting fields
  ===========
  A From feature source
    must apply filter $fromFilter ${tag("slow")}

  A Join feature source
    must apply filter $joinFilter ${tag("slow")}

  A LeftJoin feature source
    must apply filter $leftJoinFilter ${tag("slow")}
"""

  // FIXME: Pull up to test project for use by client apps
  case class TestDataSource[T](testData: Iterable[T]) extends DataSource[T, TypedPipe] {
    def load = IterablePipe(testData)
  }

  def fromFilter = forAll { (cs: List[Customer]) => {
    val filter = (c: Customer) => c.age < 18
    val source = From[Customer].filter(filter)

    val loaded = source.bind(from(TestDataSource(cs))).load
    runsSuccessfully(loaded) must_== cs.filter(filter)
  }}.set(minTestsOk = 10)

  def joinFilter = forAll { (customerAccounts: CustomerAccounts) => {
    val cas = customerAccounts.cas
    def filter(ca: (Customer, Account)) = ca._1.age < 18

    val expected = cas.flatMap(ca => ca.as.map(a => (ca.c, a))).filter(filter)

    val source = Join[Customer].to[Account].on(_.id, _.customerId).filter(filter)
    val bound = source.bind(join(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))))
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)

  def leftJoinFilter = forAll { (customerAccounts: CustomerAccounts) => {
    val cas = customerAccounts.cas
    def filter(ca: (Customer, Option[Account])) = ca._1.age < 18

    val expected = cas.flatMap(ca =>
      ca.as.toList.toNel.map(as =>
        as.list.map(a => (ca.c, a.some))
      ).getOrElse(List((ca.c, None)))
    ).filter(filter)

    val source = Join.left[Customer].to[Account].on(_.id, _.customerId).filter(filter)
    val bound = source.bind(leftJoin(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))))
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)
}
