package commbank.coppersmith.scalding

import com.twitter.scalding.TDsl.sourceToTypedPipe
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{MultipleTextLineFiles, TextLineScheme, TupleConverter, TupleSetter}

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec.DecodeOk

import commbank.coppersmith.DataSource

object ScaldingDataSource {
  object Partitions {
    def apply[P : PathComponents](underlying: Partition[_, P], values: P*): Partitions[P] =
      Partitions(underlying.pattern, values: _*)
  }
  case class Partitions[P : PathComponents](pattern: String, values: P*) {
    def toPaths(basePath: Path): List[Path] = values.map(value =>
      new Path(basePath, pattern.format(implicitly[PathComponents[P]].toComponents((value)): _*))
    ).toList
  }

  case class PathComponents[P](toComponents: P => List[String])
  import shapeless.syntax.std.tuple.productTupleOps
  implicit val StringToPath       = PathComponents[String](List(_))
  implicit val StringTuple2ToPath = PathComponents[(String, String)](_.toList)
  implicit val StringTuple3ToPath = PathComponents[(String, String, String)](_.toList)
  implicit val StringTuple4ToPath = PathComponents[(String, String, String, String)](_.toList)
}

import ScaldingDataSource.Partitions

case class HiveTextSource[S <: ThriftStruct : Decode, P](
  basePath:   Path,
  partitions: Partitions[P],
  delimiter:  String = "|"
) extends DataSource[S, TypedPipe] {
  def load = {
    val decoder = implicitly[Decode[S]]
    val input: TextLineScheme = MultipleTextLineFiles(partitions.toPaths(basePath).map(_.toString): _*)
    input.map { raw =>
      decoder.decode(none = "\\N", Splitter.delimited(delimiter).run(raw).toList)
    }.collect {
      // FIXME: This implementation completely ignores errors
      case DecodeOk(row) => row
    }
  }
}

case class HiveParquetSource[S <: ThriftStruct : Manifest : TupleConverter : TupleSetter, P](
  basePath:   Path,
  partitions: Partitions[P]
) extends DataSource[S, TypedPipe] {
  def load = {
    TypedPipe.from(ParquetScroogeSource[S](partitions.toPaths(basePath).map(_.toString): _*))
  }
}
