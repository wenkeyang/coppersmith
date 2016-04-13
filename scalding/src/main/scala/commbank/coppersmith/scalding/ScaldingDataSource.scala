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

import com.twitter.scalding.TDsl.sourceToTypedPipe
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{MultipleTextLineFiles, TextLineScheme, TupleConverter, TupleSetter}

import scalaz.syntax.std.list.ToListOpsFromList

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec.{DecodeOk, DecodeError, ParseError, NotEnoughInput, TooMuchInput}

import commbank.coppersmith.DataSource

object ScaldingDataSource {
  object Partitions {
    def apply[P : PathComponents](underlying: Partition[_, P], first: P, rest: P*): Partitions[P] =
      Partitions(underlying.pattern, first, rest: _*)

    def apply[P : PathComponents](pattern: String, first: P, rest: P*): Partitions[P] =
      Partitions(pattern, (first +: rest).toList)

    def unpartitioned = Partitions[Nothing]("", List())
  }
  case class Partitions[P : PathComponents] private(pattern: String, values: List[P]) {
    def toPaths(basePath: Path): List[Path] =
      oPaths.map(_.map(new Path(basePath, _))).getOrElse(List(new Path(basePath, "*")))

    def relativePaths: List[Path] = oPaths.getOrElse(List(new Path(".")))

    def oPaths: Option[List[Path]] = values.toNel.map(_.list.map(value =>
     new Path(pattern.format(implicitly[PathComponents[P]].toComponents((value)): _*))
    ))
  }

  case class PathComponents[P](toComponents: P => List[String])
  import shapeless.syntax.std.tuple.productTupleOps
  implicit val EmptyToPath        = PathComponents[Nothing](List())
  implicit val StringToPath       = PathComponents[String](List(_))
  implicit val StringTuple2ToPath = PathComponents[(String, String)](_.toList)
  implicit val StringTuple3ToPath = PathComponents[(String, String, String)](_.toList)
  implicit val StringTuple4ToPath = PathComponents[(String, String, String, String)](_.toList)
}

import ScaldingDataSource.Partitions

case class HiveTextSource[S <: ThriftStruct : Decode](
  paths: List[Path],
  delimiter:  String
) extends DataSource[S, TypedPipe] {
  def load = {
    val decoder = implicitly[Decode[S]]
    val input: TextLineScheme = MultipleTextLineFiles(paths.map(_.toString): _*)
    input.map { raw =>
      decoder.decode(none = "\\N", Splitter.delimited(delimiter).run(raw).toList)
    }.collect {
      case DecodeOk(row)            => row
      case e @ DecodeError(_, _, _) =>
        throw new Exception("Cannot decode input to HiveTextSource: " + errorMessage(e))
    }
  }

  def errorMessage(e: DecodeError[_]): String = e.reason match {
    // Error messages copied from maestro's LoadExecution.scala
    case ParseError(_, _, _)  => s"unexpected type: $e"
    case NotEnoughInput(_, _) => s"not enough fields in record: $e"
    case TooMuchInput         => s"too many fields in record: $e"
  }
}

object HiveTextSource {
  def apply[S <: ThriftStruct : Decode, P](
    basePath: Path,
    partitions: Partitions[P],
    delimiter: String = "|"
  ): HiveTextSource[S] =
    HiveTextSource[S](partitions.toPaths(basePath), delimiter)
}

case class HiveParquetSource[S <: ThriftStruct : Manifest : TupleConverter : TupleSetter](
  paths: List[Path]
) extends DataSource[S, TypedPipe] {
  def load = {
    ParquetScroogeSource[S](paths.map(_.toString): _*)
  }
}

object HiveParquetSource {
  def apply[S <: ThriftStruct : Manifest : TupleConverter : TupleSetter , P](
    basePath: Path,
    partitions: Partitions[P]
  ): HiveParquetSource[S] =
    HiveParquetSource[S](partitions.toPaths(basePath))
}

/** Akin to an SQL view, allow features to be derived from an arbitrary [[TypedPipe]] */
case class TypedPipeSource[T](pipe: TypedPipe[T]) extends DataSource[T, TypedPipe] {
  def load = pipe
}
