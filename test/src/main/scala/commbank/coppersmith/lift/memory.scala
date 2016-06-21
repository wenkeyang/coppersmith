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

package commbank.coppersmith.lift

import scalaz.{Order, Scalaz}, Scalaz._

import commbank.coppersmith._
import commbank.coppersmith.Feature.Value
import commbank.coppersmith.lift.generated.GeneratedMemoryLift

trait MemoryLift extends Lift[List] with GeneratedMemoryLift {
  def lift[S,V <: Value](f:Feature[S,V])(s: List[S]): List[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: List[S]): List[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, List]](
    underlying: U,
    binder:     B,
    filter:     Option[S => Boolean]
  ) = MemoryBoundFeatureSource(underlying, binder, filter)

  def liftFilter[S](p: List[S], f: S => Boolean) = p.filter(f)
}

object memory extends MemoryLift {
  implicit def framework: Lift[List] = this
}


object MemoryLift {
  def innerJoin[S1, S2, J : Ordering](
    f1: S1 => J,
    f2: S2 => J,
    s1: List[S1],
    s2: List[S2]
  ): List[(S1, S2)] = {
    implicit val orderJ: Order[J] = Order.fromScalaOrdering[J]
    val map1: Map[J, Iterable[S1]] = s1.groupBy(f1)
    val map2: Map[J, Iterable[S2]] = s2.groupBy(f2)
    for {
      (k1, s1s) <- map1.toList
      (k2, s2s) <- map2.toList if (k1 === k2)
      s1        <- s1s
      s2        <- s2s
    } yield (s1, s2)
  }

  def leftJoin[S1, S2, J : Ordering](
    f1: S1 => J,
    f2: S2 => J,
    s1: List[S1],
    s2: List[S2]
  ): List[(S1, Option[S2])] = {
    val map1: Map[J, Iterable[S1]] = s1.groupBy(f1)
    val map2: Map[J, Iterable[S2]] = s2.groupBy(f2)
    map1.toList.flatMap { case (k1, s1s) =>
      s1s.flatMap(s1 =>
        map2.get(k1).map(s2s =>
          s2s.map(s2 => (s1, s2.some))
        ).getOrElse(List((s1, None)))
      )
    }
  }
}

import memory.framework

case class MemoryBoundFeatureSource[S, U <: FeatureSource[S, U]](
  underlying: U,
  binder:     SourceBinder[S, U, List],
  filter:     Option[S => Boolean]
) extends BoundFeatureSource[S, List] {
  def load: List[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}
