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

import org.apache.commons.lang3.StringEscapeUtils

import org.slf4j.LoggerFactory

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec.{DecodeOk, DecodeError, ParseError, NotEnoughInput, TooMuchInput}

import commbank.coppersmith.DataSource

import CoppersmithStats.fromTypedPipe

/** Scalding data sources should extend this (rather than `DataSource` directly).
  * It provides some high level utility methods, such as filtering.
  */
trait ScaldingDataSource[S] extends DataSource[S, TypedPipe] {
  /** Apply a filter to the raw data source.
    * Prefer [[commbank.coppersmith.FeatureBuilder.where]], since that allows the filter
    * to be expressed in the feature definition, rather than the config.
    * This method, however, may result in better performance in some cases,
    * especially when the data source will be joined to another,
    * since the filter will be applied before rather than after the join.
    */
  def where(condition: S => Boolean) = TypedPipeSource(load.filter(condition))

  def distinct(implicit ord: Ordering[_ >: S]) = TypedPipeSource(load.distinct)

  def distinctBy[O : Ordering](fn: S => O) = TypedPipeSource(load.distinctBy(fn))
}

case class DataSourceView[T, S](underlying: DataSource[T, TypedPipe])(implicit tToS: T => S)
    extends ScaldingDataSource[S] {
  def load = underlying.load.map(tToS)
}

case class HiveTextSource[S <: ThriftStruct : Decode](
  paths:     List[Path],
  delimiter: String
) extends ScaldingDataSource[S] {
  val log = LoggerFactory.getLogger(getClass())

  def load = {
    log.info(s"Loading '${StringEscapeUtils.escapeJava(delimiter)}' delimited text from " + paths.mkString(","))
    val decoder = implicitly[Decode[S]]
    val input: TextLineScheme = MultipleTextLineFiles(paths.map(_.toString): _*)
    input.map { raw =>
      decoder.decode(none = "\\N", Splitter.delimited(delimiter).run(raw).toList)
    }.collect {
      case DecodeOk(row)            => row
      case e @ DecodeError(_, _, _) =>
        throw new Exception("Cannot decode input to HiveTextSource: " + errorMessage(e))
    }.withCounter("text.loaded")
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
) extends ScaldingDataSource[S] {
  val log = LoggerFactory.getLogger(getClass())

  def load = {
    log.info("Loading parquet from " + paths.mkString(","))
    TypedPipe.from(ParquetScroogeSource[S](paths.map(_.toString): _*))
      .withCounter("parquet.loaded")
  }
}

object HiveParquetSource {
  def apply[S <: ThriftStruct : Manifest : TupleConverter : TupleSetter, P](
    basePath: Path,
    partitions: Partitions[P]
  ): HiveParquetSource[S] =
    HiveParquetSource[S](partitions.toPaths(basePath))
}

/** Akin to an SQL view, allow features to be derived from an arbitrary `TypedPipe`. */
case class TypedPipeSource[S](pipe: TypedPipe[S]) extends ScaldingDataSource[S] {
  val log = LoggerFactory.getLogger(getClass())

  def load = {
    log.info(s"Loading from a TypedPipeSource")
    pipe
      .withCounter("typedpipe.loaded")
  }
}
