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

import scalaz.std.list.listInstance

import shapeless._
import shapeless.ops.hlist._

import commbank.coppersmith._
import commbank.coppersmith.Feature.Value

trait MemoryLift extends Lift[List] {
  def lift[S,V <: Value](f:Feature[S,V])(s: List[S]): List[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: List[S]): List[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  type +:[A <: HList, B] =  Prepend[A, B :: HNil]


  def innerJoinNext[LeftSides <: HList, RightSide, J : Ordering, Out <: HList]
  (l: LeftSides => J, r: RightSide => J )
  (a:List[LeftSides], b: List[RightSide])
  (implicit pp: Prepend.Aux[LeftSides, RightSide :: HNil, Out])
  : List[Out] = {
    val aMap: Map[J, List[LeftSides]] = a.groupBy(l)
    val bMap: Map[J, List[RightSide]] = b.groupBy(r)

    val result = for {
      (k1,v1) <- aMap.toList
      (k2,v2) <- bMap.toList if k2 == k1
      a <- v1
      b <-v2
    } yield a :+ b

    result
  }


  override def leftJoinNext[LeftSides <: HList, RightSide, J : Ordering, Out <: HList]
  (l: LeftSides => J, r: RightSide => J )
  (as:List[LeftSides], bs: List[RightSide])
  (implicit pp: Prepend.Aux[LeftSides, Option[RightSide] :: HNil, Out])
  : List[Out] =
    as.flatMap { a =>
      val leftKey = l(a)
      val rightValues = bs.filter {b => r(b) == leftKey}
      if (rightValues.isEmpty) {
        List(a :+ (None : Option[RightSide]))
      } else {
        rightValues.map {b => a :+ (Some(b) : Option[RightSide])}
      }
    }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, List]]
    (underlying: U, binder: B, filter: Option[S => Boolean]) =
      MemoryBoundFeatureSource(underlying, binder, filter)

  def liftFilter[S](p: List[S], f: S => Boolean) = p.filter(f)
}

object memory extends MemoryLift {
  implicit def framework: Lift[List] = this
}

import memory.framework

case class MemoryBoundFeatureSource[S, U <: FeatureSource[S, U]](
  underlying: U,
  binder: SourceBinder[S, U, List],
  filter: Option[S => Boolean]
) extends BoundFeatureSource[S, List] {
  def load: List[S] = {
    val pipe = binder.bind(underlying)
    filter.map(f => pipe.filter(f)).getOrElse(pipe)
  }
}
