//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith

package scalding

import com.twitter.scalding.typed._

import org.scalacheck.Arbitrary, Arbitrary.arbitrary
import org.scalacheck.Prop.forAll

import shapeless.Generic

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
    must apply filter             $fromFilter        ${tag("slow")}
    must map context              $fromContext       ${tag("slow")}
    must apply filter withContext $fromContextFilter ${tag("slow")}

  A Join feature source
    must apply filter             $joinFilter        ${tag("slow")}
    must map context              $joinContext       ${tag("slow")}
    must apply filter withContext $joinContextFilter ${tag("slow")}

  A LeftJoin feature source
    must apply filter             $leftJoinFilter        ${tag("slow")}
    must map context              $leftJoinContext       ${tag("slow")}
    must apply filter withContext $leftJoinContextFilter ${tag("slow")}

  A multiway join feature source
    must have correct results     $multiwayJoin              ${tag("slow")}
    must apply filter             $multiwayJoinFilter        ${tag("slow")}
    must map context              $multiwayJoinContext       ${tag("slow")}
    must apply filter withContext $multiwayJoinContextFilter ${tag("slow")}
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

  def fromContext = forAll { (cs: List[Customer], ctx: String) => {
    val source = From[Customer].withContext[String]

    val loaded = source.bindWithContext(from(TestDataSource(cs)), ctx).load
    runsSuccessfully(loaded) must_== cs.map((_, ctx))
  }}.set(minTestsOk = 10)

  def fromContextFilter = forAll { (cs: List[Customer], ctx: Boolean) => {
    def filter(cb: (Customer, Boolean)) = cb._1.age < 18 || cb._2
    val source = From[Customer].withContext[Boolean].filter(filter)

    val loaded = source.bindWithContext(from(TestDataSource(cs)), ctx).load
    runsSuccessfully(loaded) must_== cs.map((_, ctx)).filter(filter)
  }}.set(minTestsOk = 10)

  implicit val arbCustAccts: Arbitrary[CustomerAccounts] = arbCustomerAccounts(arbitrary[String])

  def joinFilter = forAll { (customerAccounts: CustomerAccounts) => {
    val cas = customerAccounts.cas
    def filter(ca: (Customer, Account)) = ca._1.age < 18

    val expected = cas.flatMap(ca => ca.as.map(a => (ca.c, a))).filter(filter)

    val source = Join[Customer].to[Account].on(_.id, _.customerId).filter(filter)
    val bound = source.bind(join(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))))
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)

  def joinContext = forAll { (customerAccounts: CustomerAccounts, ctx: String) => {
    val cas = customerAccounts.cas

    val expected = cas.flatMap(ca => ca.as.map(a => ((ca.c, a), ctx)))

    val source = Join[Customer].to[Account].on(_.id, _.customerId).withContext[String]
    val bound = source.bindWithContext(
      join(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))),
      ctx
    )
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)

  def joinContextFilter = forAll { (customerAccounts: CustomerAccounts, ctx: Boolean) => {
    val cas = customerAccounts.cas
    def filter(cab: ((Customer, Account), Boolean)) = cab._1._1.age < 18 || cab._2

    val expected = cas.flatMap(ca => ca.as.map(a => ((ca.c, a), ctx))).filter(filter)

    val source = Join[Customer].to[Account].on(
      _.id,
      _.customerId
    ).withContext[Boolean].filter(filter)

    val bound = source.bindWithContext(
      join(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))),
      ctx
    )
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

  def leftJoinContext = forAll { (customerAccounts: CustomerAccounts, ctx: String) => {
    val cas = customerAccounts.cas

    val expected = cas.flatMap(ca =>
      ca.as.toList.toNel.map(as =>
        as.list.map(a => (ca.c, a.some))
      ).getOrElse(List((ca.c, None)))
    ).map((_, ctx))

    val source = Join.left[Customer].to[Account].on(_.id, _.customerId).withContext[String]
    val bound = source.bindWithContext(
      leftJoin(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))),
      ctx
    )
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)

  def leftJoinContextFilter = forAll { (customerAccounts: CustomerAccounts, ctx: Boolean) => {
    val cas = customerAccounts.cas
    def filter(cab: ((Customer, Option[Account]), Boolean)) = cab._1._1.age < 18 || cab._2

    val expected = cas.flatMap(ca =>
      ca.as.toList.toNel.map(as =>
        as.list.map(a => (ca.c, a.some))
      ).getOrElse(List((ca.c, None)))
    ).map((_, ctx)).filter(filter)

    val source = Join.left[Customer].to[Account].on(
      _.id,
      _.customerId
    ).withContext[Boolean].filter(filter)

    val bound = source.bindWithContext(
      leftJoin(TestDataSource(cas.map(_.c)), TestDataSource(cas.flatMap(_.as))),
      ctx
    )
    runsSuccessfully(bound.load).toSet must_== expected.toSet
  }}.set(minTestsOk = 10)

  def multiwayJoin = forAll { (customerAccounts: CustomerAccounts) => {
    //shadow accidentally imported implicit
    implicit val genMonad = 1

    val cas = customerAccounts.cas

    val expected = cas.flatMap(ca => ca.as.map(a => (ca.c, a)))

    val source = Join.multiway[Customer].inner[Account].on(
      (c: Customer) => c.id,
      (a: Account) => a.customerId
    )

    val customersDs: DataSource[Customer, TypedPipe] = TestDataSource(cas.map(_.c))
    val accountsDs: DataSource[Account, TypedPipe] = TestDataSource(cas.flatMap(_.as))

    val bound = source.bind(joinInner(customersDs, accountsDs))

    runsSuccessfully(bound.load).toSet must_== expected.toSet

  }}.set(minTestsOk = 10)

  def multiwayJoinFilter = forAll { (customerAccounts: CustomerAccounts) => {
    //shadow accidentally imported implicit
    implicit val genMonad = 1

    val cas = customerAccounts.cas
    def filter(ca: (Customer, Account)) = ca._1.age < 18

    val expected = cas.flatMap(ca => ca.as.map(a => (ca.c, a))).filter(filter)

    val source = Join.multiway[Customer].inner[Account].on(
      (c: Customer) => c.id,
      (a: Account) => a.customerId
    ).filter(filter)

    val customersDs: DataSource[Customer, TypedPipe] = TestDataSource(cas.map(_.c))
    val accountsDs:  DataSource[Account,  TypedPipe] = TestDataSource(cas.flatMap(_.as))

    val bound = source.bind(joinInner(customersDs, accountsDs))

    runsSuccessfully(bound.load).toSet must_== expected.toSet

  }}.set(minTestsOk = 10)

  def multiwayJoinContext =
    pending("Missing generality in multiway join binder currently prevents withContext from compiling")

  def multiwayJoinContextFilter =
    pending("Missing generality in multiway join binder currently prevents withContext from compiling")
}
