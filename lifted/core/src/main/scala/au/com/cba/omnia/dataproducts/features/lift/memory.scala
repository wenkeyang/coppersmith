package au.com.cba.omnia.dataproducts.features.lift

import au.com.cba.omnia.dataproducts.features.Join._
import au.com.cba.omnia.dataproducts.features._

import au.com.cba.omnia.dataproducts.features.Feature.Value

trait MemoryLift extends Lift[List] with Materialise[List, ({type λ[-α] = α => Unit})#λ, ({type λ[α] = Unit => α})#λ]{
  def liftToTypedPipe[S,V <: Value](f:Feature[S,V])(s: List[S]): List[FeatureValue[S, V]] = {
    s.flatMap(s => f.generate(s))
  }

  def liftToTypedPipe[S](fs: FeatureSet[S])(s: List[S]): List[FeatureValue[S, _]] = {
    s.flatMap(s => fs.generate(s))
  }


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J])(a:List[A], b: List[B]): List[(A, B)] = {
    val aMap: Map[J, List[A]] = a.groupBy(joined.left)
    val bMap: Map[J, List[B]] = b.groupBy(joined.right)

    aMap.flatMap { case (k, as) =>
      if (bMap.contains(k)) {
        as zip bMap(k)
      } else {
        List()
      }
    }.toList
  }

  def materialiseJoinFeature[A, B, J : Ordering, V <: Value]
  (joined: Joined[A, B, J], feature: Feature[(A,B),V])
  (leftSrc:List[A], rightSrc:List[B], sink: (FeatureValue[(A,B),V]) => Unit) =
    materialise[(A,B), V](feature)(liftJoin(joined)(leftSrc, rightSrc), sink)

  def materialise[S,V <: Value](f:Feature[S,V])(src:List[S], sink: FeatureValue[S,V] => Unit): Unit => Unit = _ => {
    liftToTypedPipe(f)(src).foreach(sink)
  }

  def materialise[S](featureSet: FeatureSet[S])(src:List[S], sink: FeatureValue[S,_] => Unit): Unit => Unit = _ => {
    liftToTypedPipe(featureSet)(src).foreach(sink)
  }
}
object memory extends MemoryLift
