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

import commbank.coppersmith.generated.Joined2

import scala.reflect.ClassTag

object From {
  def apply[S](): From[S] = From(None)
}

case class From[S](filter: Option[S => Boolean] = None) extends FeatureSource[S, From[S]](filter) {
  type FS = From[S]

  def copyWithFilter(filter: Option[S => Boolean]) = copy(filter)
}

object Join {
  def join[T]: InnerJoinableTo[T] = new InnerJoinableTo[T]
  def apply[T] = join[T]

  def left[T]: LeftOuterJoinableTo[T] = new LeftOuterJoinableTo[T]

  def multiway[T]: JoinableTo[T] = new JoinableTo[T]

  class InnerJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, L, R] = new IncompleteJoin[L, R, L, R]
  }

  class LeftOuterJoinableTo[L] {
    def to[R]: IncompleteJoin[L, R, L, Option[R]] = new IncompleteJoin[L, R, L, Option[R]]
  }

  class JoinableTo[L] {
    def inner[R]: IncompleteJoin[L, R, L, R]         = new IncompleteJoin[L, R, L, R]
    def left[R]:  IncompleteJoin[L, R, L, Option[R]] = new IncompleteJoin[L, R, L, Option[R]]
  }

  class IncompleteJoin[S1, S2, T1, T2] {
    def on[J: Ordering : ClassTag](s1: T1 => J, s2: S2 => J): Joined2[S1, S2, J, T1, T2] = Joined2(s1, s2, None)
  }
}
