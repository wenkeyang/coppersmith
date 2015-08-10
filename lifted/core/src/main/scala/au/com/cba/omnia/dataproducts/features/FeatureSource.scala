package au.com.cba.omnia.dataproducts.features

import org.apache.hadoop.fs.Path

import com.twitter.scalding.{Config, Execution, MultipleTextLineFiles, TupleSetter, TupleConverter}
import com.twitter.scalding.typed._

import au.com.cba.omnia.maestro.api._

import au.com.cba.omnia.etl.util.{ParseUtils, SimpleMaestroJob}

trait FeatureSource[S] {
  def filter(p: S => Boolean): FeatureSource[S]
  def load(conf: FeatureJobConfig): Execution[TypedPipe[S]]
  def groupBy[T](f: S => T): FeatureSource[(T, Iterable[S])] = ???
}


case class HiveTextSource[S <: ThriftStruct : Decode, P](
  tableName: String,
  partition: Partition[S, P],
  filter: S => Boolean = (_: S) => true
) extends FeatureSource[S] {
  def filter(f: S => Boolean): HiveTextSource[S, P] = copy(filter = (s: S) => filter(s) && f(s))
  def load(conf: FeatureJobConfig) = Execution.from {
    // FIXME: This implementation completely ignores errors
    ParseUtils.decodeHiveTextTable[S](
      MultipleTextLineFiles(conf.sourcePath.toString + tableName /* + partition...*/)
    ).rows.filter(filter)
  }
}
