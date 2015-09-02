package commbank.coppersmith.scalding

import com.twitter.scalding.TDsl.sourceToTypedPipe
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding.{MultipleTextLineFiles, TextLineScheme, TupleConverter, TupleSetter}

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import au.com.cba.omnia.maestro.api._
import au.com.cba.omnia.maestro.core.codec.DecodeOk


object DataSource {
  case class PartitionPath[S, P](underlying: Partition[S, P], value: P)(implicit ev: PartitionToPath[P]) {
    def toPath = new Path(underlying.pattern.format(ev.toPathComponents((value)): _*))
  }

  case class PartitionToPath[P](toPathComponents: P => List[String])
  import shapeless.syntax.std.tuple.productTupleOps
  implicit val StringToPath       = PartitionToPath[String](List(_))
  implicit val StringTuple2ToPath = PartitionToPath[(String, String)](_.toList)
  implicit val StringTuple3ToPath = PartitionToPath[(String, String, String)](_.toList)
  implicit val StringTuple4ToPath = PartitionToPath[(String, String, String, String)](_.toList)
}

trait DataSource[S] {
  def load: TypedPipe[S]
}

case class HiveTextSource[S <: ThriftStruct : Decode, P](
  basePath:  Path,
  partition: DataSource.PartitionPath[S, P],
  delimiter: String = "|",
  filter:    S => Boolean = (_: S) => true
) extends DataSource[S] {
  def filter(f: S => Boolean): HiveTextSource[S, P] = copy(filter = (s: S) => filter(s) && f(s))
  def load = {
    val ev = implicitly[Decode[S]]
    val input: TextLineScheme = MultipleTextLineFiles(new Path(basePath, partition.toPath).toString)
    input.map { raw =>
      ev.decode(none = "\\N", Splitter.delimited(delimiter).run(raw).toList)
    }.collect {
      // FIXME: This implementation completely ignores errors
      case DecodeOk(row) if filter(row) => row
    }
  }
}

case class HiveParquetSource[S <: ThriftStruct : Manifest : TupleConverter : TupleSetter, P](
  basePath:  Path,
  partition: DataSource.PartitionPath[S, P],
  filter:    S => Boolean = (_: S) => true
) extends DataSource[S] {
  def filter(f: S => Boolean): HiveParquetSource[S, P] = copy(filter = (s: S) => filter(s) && f(s))
  def load = {
    TypedPipe.from(ParquetScroogeSource[S](new Path(basePath, partition.toPath).toString))
  }
}
