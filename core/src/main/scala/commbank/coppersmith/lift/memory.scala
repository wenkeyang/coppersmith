package commbank.coppersmith.lift

import commbank.coppersmith._
import commbank.coppersmith.Join._

import commbank.coppersmith.Feature.Value

trait MemoryLift extends Lift[List] {
  def lift[S,V <: Value](f:Feature[S,V])(s: List[S]): List[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: List[S]): List[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, B), Inner[A, B]])(a:List[A], b: List[B]): List[(A, B)] = {
    val aMap: Map[J, List[A]] = a.groupBy(joined.left)
    val bMap: Map[J, List[B]] = b.groupBy(joined.right)

    for {
      (k1,v1) <- aMap.toList
      (k2,v2) <- bMap.toList if k2 == k1
      a <- v1
      b <-v2
    } yield (a, b)
  }

  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, (A, Option[B]), LeftOuter[A, B]])(as: List[A], bs: List[B]): List[(A, Option[B])] =
    as.flatMap { a =>
      val leftKey = joined.left(a)
      val rightValues = bs.filter {b => joined.right(b) == leftKey}
      if (rightValues.isEmpty) {
        List((a, None))
      } else {
        rightValues.map {b => (a, Some(b))}
      }
    }

  def liftBinder[S, U, B <: SourceBinder[S, U, List]](underlying: U, binder: B, filter: Option[S => Boolean]) =
    MemoryConfiguredFeatureSource(underlying, binder, filter)

  case class MemoryConfiguredFeatureSource[S, U](
    underlying: U,
    binder: SourceBinder[S, U, List],
    filter: Option[S => Boolean]
  ) extends ConfiguredFeatureSource[S, U, List] {
    def load: List[S] = {
      val pipe = binder.bind(underlying)
      filter.map(f => pipe.filter(f)).getOrElse(pipe)
    }
  }

}
object memory extends MemoryLift
