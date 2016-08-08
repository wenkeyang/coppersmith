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

package commbank.coppersmith.scalding

import scalaz.NonEmptyList
import scalaz.syntax.std.list.ToListOpsFromList

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api.Partition

import Partitions.PathComponents

/** A concrete set of partition values that can be used to construct a relative path
  * based on a pattern that incorporates those values as format args. Use the various
  * `Partitions.apply` methods to create instances, or the `unpartitioned` value for
  * unpartitioned data.
  */
case class Partitions[P : PathComponents] private (pattern: String, values: List[P]) {

  /** Paths relative to the supplied `basePath` */
  def toPaths(basePath: Path): List[Path] =
    oPaths.map(_.map(new Path(basePath, _))).map(_.list).getOrElse(List(new Path(basePath, "*")))

  def relativePaths: List[Path] = oPaths.map(_.list).getOrElse(List(new Path(".")))

  /** Relative paths that this instance represents, or `None` if `values` is empty. */
  def oPaths: Option[NonEmptyList[Path]] =
    values.toNel.map(_.map(value =>
              new Path(pattern.format(implicitly[PathComponents[P]].toComponents((value)): _*))))
}

object Partitions {
  def apply[P : PathComponents](underlying: Partition[_, P], first: P, rest: P*): Partitions[P] =
    Partitions(underlying.pattern, first, rest: _*)

  def apply[P : PathComponents](pattern: String, first: P, rest: P*): Partitions[P] =
    Partitions(pattern, (first +: rest).toList)

  def unpartitioned = Partitions[Nothing]("", List())(PathComponents[Nothing](List()))

  case class PathComponents[P](toComponents: P => List[String])

  object PathComponents {
    import shapeless.syntax.std.tuple.productTupleOps
    implicit val StringToPath       = PathComponents[String](List(_))
    implicit val StringTuple2ToPath = PathComponents[(String, String)](_.toList)
    implicit val StringTuple3ToPath = PathComponents[(String, String, String)](_.toList)
    implicit val StringTuple4ToPath = PathComponents[(String, String, String, String)](_.toList)
  }
}
