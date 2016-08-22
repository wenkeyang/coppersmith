package commbank.coppersmith
package spark
package lift

import generated.GeneratedSparkLift
import org.apache.spark.rdd._
import scala.reflect.ClassTag

object functor {
  implicit def rddFunctor: SerializableFunctor[RDD] = new SerializableFunctor[RDD] {
    def map[A, B : ClassTag](fa: RDD[A])(f: A => B): RDD[B] = fa.map(f)
  }
}

import functor._

trait SparkLift extends Lift [RDD] with GeneratedSparkLift {
  def liftFilter[S](p: RDD[S], f: S => Boolean): RDD[S] = p.filter(f)

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S,U,RDD]]
    (underlying: U,binder: B,filter: Option[S => Boolean]): BoundFeatureSource[S, RDD] =
      SparkBoundFeatureSource(underlying, binder, filter)

   def lift[S](fs: FeatureSet[S])(s: RDD[S]): RDD[commbank.coppersmith.FeatureValue[_]] =
       s.flatMap(s => fs.generate(s))
   def lift[S, V <: Feature.Value](f: Feature[S,V])(s: RDD[S]): RDD[FeatureValue[V]] =
       s.flatMap(s => f.generate(s))
}


object spark extends SparkLift
