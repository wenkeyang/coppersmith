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

package commbank.coppersmith.scalding

import org.specs2.execute._, Typecheck._

import org.scalacheck.Prop.forAll

import cascading.flow.FlowException

import com.twitter.scalding.typed.TypedPipe

import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import au.com.cba.omnia.maestro.api._, Maestro._

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.core.ThermometerSpec

import commbank.coppersmith.api._
import commbank.coppersmith.{DataSource, BoundFeatureSource}

import commbank.coppersmith.test.thrift.{Customer, Account}
import commbank.coppersmith.Arbitraries._

class HiveTextSourceSpec extends ThermometerSpec { def is = s2"""
  HiveTextSource
    should read multiple listed partitions   $multiplePartitions
    should read multiple globbed partitions  $globPartitions
    should read unpartitioned table          $unpartitioned
    should throw exception on decode failure $decodeFailure
"""
  def multiplePartitions = {
    val partitions = Partitions("status=%s", "ACTIVE", "INACTIVE")
    val dataSource = HiveTextSource[Customer, String](path("multiplePartitions"), partitions)

    // TODO: Use scalacheck instead of relying on these values being in sync with test files
    val expected = List(
      Customer("active_id", "active_name", 19, 1.5, None, 12345),
      Customer("inactive_id", "inactive_name", 21, 1.6, None, 54321)
    )

    withEnvironment(path(getClass.getResource("/hiveTextSource").toString)) {
      runsSuccessfully(dataSource.load).toSet must_== expected.toSet
    }
  }

  def globPartitions = {
    val partitions = Partitions("status=%s", "*")
    val dataSource = HiveTextSource[Customer, String](path("multiplePartitions"), partitions)

    val expected = List(
      Customer("active_id", "active_name", 19, 1.5, None, 12345),
      Customer("inactive_id", "inactive_name", 21, 1.6, None, 54321)
    )

    withEnvironment(path(getClass.getResource("/hiveTextSource").toString)) {
      runsSuccessfully(dataSource.load).toSet must_== expected.toSet
    }
  }

  def unpartitioned = {
    val partitions = Partitions.unpartitioned
    val dataSource = HiveTextSource[Customer, Nothing](path("unpartitioned"), partitions)

    val expected = List(
      Customer("unpart_id", "unpart_name", 19, 1.5, None, 12345)
    )

    withEnvironment(path(getClass.getResource("/hiveTextSource").toString)) {
      runsSuccessfully(dataSource.load).toSet must_== expected.toSet
    }
  }

  def decodeFailure = {
    // reuse input data from [[unpartitioned]], but attempt to decode using Account thrift
    val dataSource = HiveTextSource[Account, Nothing](path("unpartitioned"), Partitions.unpartitioned)

    withEnvironment(path(getClass.getResource("/hiveTextSource").toString)) {
      // Suppress error that is normally logged by the framework when the expected exception is raised
      TestUtil.withoutLogging("cascading.flow.stream") {

        // this wraps the actual coppersmith exception
        run(dataSource.load) must beFailedTry.withThrowable[FlowException]
      }
    }
  }
}

object TypedPipeSourceSpec extends ThermometerSpec { def is = s2"""
  TypedPipeSource
    should bind to a From[T]       $bind
    should load from a typed pipe  $load
"""
  def bind = typecheck { """
    import scalding._
    val dataSource = TypedPipeSource[Customer](TypedPipe.empty)
    val bound: BoundFeatureSource[Customer, TypedPipe] = From[Customer].bind(from(dataSource))
  """ }

  def load = forAll { (recs: Iterable[Customer]) =>
    val dataSource = TypedPipeSource(TypedPipe.from(recs))
    runsSuccessfully(dataSource.load).toSet must_== recs.toSet
  }
}
