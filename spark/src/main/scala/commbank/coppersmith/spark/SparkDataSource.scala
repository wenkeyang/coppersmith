package commbank.coppersmith
package spark

import org.apache.spark.sql.{SparkSession, Encoder}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path

import commonImports._

import org.slf4j.LoggerFactory

import au.com.cba.omnia.maestro.core.codec.{DecodeOk, DecodeError, ParseError, NotEnoughInput, TooMuchInput}

import scala.reflect.ClassTag

trait SparkDataSource[S] extends DataSource[S, RDD]

case class RddSource[S](rdd: RDD[S]) extends SparkDataSource[S] {
  def load = rdd
}

class HiveTextSource[S <: ThriftStruct : Decode : ClassTag](
  paths: List[Path],
  delimiter: String
)(implicit spark: SparkSession) extends SparkDataSource[S] {
  val log = LoggerFactory.getLogger(getClass())

  case class TextValue(value: String)

  def load = {
    import spark.implicits._

    log.info("Loading from " + paths.mkString(","))


    val decoder = implicitly[Decode[S]]

    val input = spark.read.textFile(paths.map(_.toString): _*).as[TextValue].rdd.map(_.value)
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
  def apply[S <: ThriftStruct : Decode : ClassTag](
    basePath: Path,
    delimiter: String = "|"
  )(implicit spark: SparkSession): HiveTextSource[S] =
    new HiveTextSource[S](List(basePath), delimiter)
}


case class HiveParquetSource[S <: ThriftStruct : Encoder](
  paths: List[Path]
)(implicit spark: SparkSession) extends SparkDataSource[S] {
  val log = LoggerFactory.getLogger(getClass())

  def load = {
    spark.read.parquet(paths.map(_.toString): _*).as[S].rdd
  }
}

object HiveParquetSource {
  def apply[S <: ThriftStruct : Encoder, P](
    basePath: Path,
    partitions: Partitions[P]
  )(implicit spark: SparkSession): HiveParquetSource[S] =
    HiveParquetSource[S](partitions.toPaths(basePath))
}
