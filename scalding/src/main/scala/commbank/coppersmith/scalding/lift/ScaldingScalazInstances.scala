package commbank.coppersmith.scalding.lift

import com.twitter.scalding.typed.TypedPipe

import scalaz.Functor

/**
 * There is good  reason to include these somewhere more central to omnia, and soon.
 */
object ScaldingScalazInstances {
  implicit val typedPipeFunctor: Functor[TypedPipe] = new Functor[TypedPipe] {
    override def map[A, B](fa: TypedPipe[A])(f: (A) => B): TypedPipe[B] = fa.map(f)
  }
}
