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
    def apply[P : PathComponents](underlying: Partition[_, P], values: P*): Partitions[P] =
      Partitions(underlying.pattern, values: _*)

    def unpartitioned = Partitions[Nothing]("")
  }
  case class Partitions[P : PathComponents](pattern: String, values: P*) {
    def toPaths(basePath: Path): List[Path] =
      oPaths.map(_.map(new Path(basePath, _))).getOrElse(List(new Path(basePath, "*")))

    def relativePaths: List[Path] = oPaths.getOrElse(List(new Path(".")))

    def oPaths: Option[List[Path]] = values.toList.toNel.map(_.list.map(value =>
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

case class HiveParquetSource[S <: ThriftStruct : Manifest : TupleConverter : TupleSetter, P](
  basePath:   Path,
  partitions: Partitions[P]
) extends DataSource[S, TypedPipe] {
  def load = {
    TypedPipe.from(ParquetScroogeSource[S](partitions.toPaths(basePath).map(_.toString): _*))
  }
}
