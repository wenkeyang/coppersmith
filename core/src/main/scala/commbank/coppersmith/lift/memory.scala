package commbank.coppersmith.lift

import commbank.coppersmith._

import commbank.coppersmith.Feature.Value
import shapeless._
import shapeless.ops.hlist._
import commbank.coppersmith._, Feature.Value


trait MemoryLift extends Lift[List] {
  def lift[S,V <: Value](f:Feature[S,V])(s: List[S]): List[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: List[S]): List[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  type +:[A <: HList, B] =  Prepend[A, B :: HNil]

  def liftJoinHl[HL <: HList, B, J : Ordering, O <: HList]
    (joined: Joined[HL, B, J, (HL, B) ])
    (a:List[HL], b: List[B])
    (implicit pp: Prepend.Aux[HL, B :: HNil, O])
      : List[O] = {
    val aMap: Map[J, List[HL]] = a.groupBy(joined.left)
    val bMap: Map[J, List[B]] = b.groupBy(joined.right)

    val result = for {
      (k1,v1) <- aMap.toList
      (k2,v2) <- bMap.toList if k2 == k1
      a <- v1
      b <-v2
    } yield a :+ b

    result
  }

  def liftLeftJoinHl[HL <: HList, B, J : Ordering, O <: HList]
    (joined: Joined[HL, B, J, (HL, Option[B])])
    (as: List[HL], bs: List[B])
    (implicit pp: Prepend.Aux[HL, Option[B] :: HNil, O])
      : List[O] =
    as.flatMap { a =>
      val leftKey = joined.left(a)
      val rightValues = bs.filter {b => joined.right(b) == leftKey}
      if (rightValues.isEmpty) {
        List(a :+ (None : Option[B]))
      } else {
        rightValues.map {b => a :+ (Some(b) : Option[B])}
      }
    }

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, List]](underlying: U, binder: B, filter: Option[S => Boolean]) =
    MemoryBoundFeatureSource(underlying, binder, filter)

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

}
object memory extends MemoryLift
