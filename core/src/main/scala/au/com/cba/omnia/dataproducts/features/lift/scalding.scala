package au.com.cba.omnia.dataproducts.features.lift

import au.com.cba.omnia.dataproducts.features.Feature.Value
import au.com.cba.omnia.dataproducts.features.Join._
import au.com.cba.omnia.dataproducts.features._
import com.twitter.scalding._

trait ScaldingLift extends Lift[TypedPipe] {

  def lift[S, V <: Value](f:Feature[S,V])(s: TypedPipe[S]): TypedPipe[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: TypedPipe[S]): TypedPipe[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }


  def liftJoin[A, B, J : Ordering](joined: Joined[A, B, J, Inner.type])
                                  (a:TypedPipe[A], b: TypedPipe[B]): TypedPipe[(A, B)] =
    a.groupBy(joined.left).join(b.groupBy(joined.right)).values


  def liftLeftJoin[A, B, J : Ordering](joined: Joined[A, B, J, LeftOuter.type])
                                  (a:TypedPipe[A], b: TypedPipe[B]): TypedPipe[(A, Option[B])] =
    a.groupBy(joined.left).leftJoin(b.groupBy(joined.right)).values

}

object scalding extends ScaldingLift
