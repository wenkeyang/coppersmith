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

import scalaz.Functor

import commbank.coppersmith.generated.{GeneratedLift, Joined2}

import Feature.Value

// Would have preferred functor to be specified as P[_] : Functor, but upcoming code in
// ContextFeatureSource.bindWithContext is unable to derive implicit Functor instance,
// and needs to access it via the Lift instance instead.
abstract class Lift[P[_]](implicit val functor: Functor[P]) extends GeneratedLift[P] {
  def lift[S, V <: Value](f: Feature[S, V])(s: P[S]): P[FeatureValue[V]]

  def lift[S](fs: FeatureSet[S])(s: P[S]): P[FeatureValue[_]]

  def liftJoin[A, B, J : Ordering](
      joined: Joined2[A, B, J, A, B]
  )(a: P[A], b: P[B]): P[(A, B)] = liftJoinInner(joined)(a, b) // From GenereatedLift

  def liftLeftJoin[A, B, J : Ordering](
      joined: Joined2[A, B, J, A, Option[B]]
  )(a: P[A], b: P[B]): P[(A, Option[B])] = liftJoinLeft(joined)(a, b) // From GenereatedLift

  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, P]](
      underlying: U,
      binder: B,
      filter: Option[S => Boolean]
  ): BoundFeatureSource[S, P]

  def liftFilter[S](p: P[S], f: S => Boolean): P[S]
}
