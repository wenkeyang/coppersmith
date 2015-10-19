package commbank.coppersmith
package tools

import scala.io.Source

import java.io.{File, FileInputStream, PrintStream}

import scalaz._, Scalaz._

/*
 * Tool for generating blank feature scala code from an existing metadata PSV file.
 *
 * Supports the following formats
 *
 *   namespace.name|valueType|featureType
 *   namespace.name,valueType,featureType,description
 *
 * where valueType is one of "double", "int" or "string" and featureType is one of
 * "continuous" or "categorical"
 *
 * Example usage:
 *
 * CoppersmithBootstrap --source-type Customer --file /path/to/metadata.psv --out Customer.scala
 *
 * If --out is missing, result is printed to stdout
 */
object CoppersmithBootstrap {
  def main(rawArgs: Array[String]): Unit = {
    val args       = parseArgs(rawArgs)
    val sourceType = args.getOrElse("source-type", sys.error("source-type arg missing"))
    val inFile     = new File(args.getOrElse("file", sys.error("file arg missing")))
    val outFile    = args.get("out").map(new File(_))

    val out = outFile.map(new PrintStream(_))
    try {
      run(sourceType, inFile, out).fold(
        e => {
          e.foreach(System.err.println)
          System.exit(-1)
        },
        _ => System.exit(0)
      )
    } finally {
      out.foreach(_.close)
    }
  }

  def parseArgs(args: Array[String]): Map[String, String] =
    args.take(2) match {
      case Array(n, v) if n.startsWith("--") => Map(n.drop(2) -> v) ++ parseArgs(args.drop(2))
      case _                                 => Map()
    }

  def run(sourceType: String, inFile: File, out: Option[PrintStream]) = {
    if (!inFile.exists) {
      s"File not found: ${inFile.getAbsolutePath}".failure.toValidationNel
    } else {
      val fileIn = new FileInputStream(inFile)
      val source = Source.fromInputStream(fileIn)
      try {
        bootstrapScala(sourceType, source).map(out.getOrElse(System.out).println(_))
      } finally {
        fileIn.close
      }
    }
  }

  type Namespace   = String
  type Name        = String
  type ValueType   = String
  type FeatureType = String
  type Description = String
  type Metadata    = (Namespace, Name, ValueType, FeatureType, Description)

  def bootstrapScala(sourceType: String, psvMetadata: Source): ValidationNel[String, String] = {
    val featureMetadata: ValidationNel[String, List[Metadata]] =
      psvMetadata.getLines.map(_.trim).zipWithIndex.filterNot { case (l, _) =>
        l.isEmpty || l.startsWith("#")
      }.toList.map {
        case (line, idx) => parseMetadata(line).leftMap(e => s"Error at line ${idx + 1}: $e").toValidationNel
      }.sequence[({type l[a]=ValidationNel[String, a]})#l, Metadata]

    featureMetadata.map(toScala(sourceType))
  }

  def parseMetadata(s: String): Validation[String, Metadata] =
    parseMetadata(s, '|').right.getOrElse(parseMetadata(s, ','))

  def parseMetadata(s: String, sep: Char): Validation[String, Metadata] = {
    val values = s.split(sep)
    val parts = values.take(3) match {
      case Array(qName, vTypeStr, fTypeStr) => (qName, vTypeStr, fTypeStr).success
      case _ => s"Could not parse separated values from '$s'".failure
    }

    import scalaz.Validation.FlatMap._
    parts.flatMap { case (qualifiedName, vTypeStr, fTypeStr) =>
      (parseName(qualifiedName) |@| parseTypes(vTypeStr, fTypeStr)) {
        case ((ns, name), (vType, fType)) => {
          val desc = if (values.size == 4) values(3) else s"Description for $name"
          (ns, name, vType, fType, desc)
        }
      }
    }
  }

  def parseName(qName: String) = qName.split('.') match {
      case Array(ns, name) => ((ns, name)).success
      case _               => s"Could not parse name from '$qName'".failure
    }

  def parseTypes(vTypeStr: String, fTypeStr: String) = {
    for {
      vType <- parseValueType(vTypeStr).toEither
      fType <- parseFeatureType(fTypeStr, vType).toEither
    } yield (vType, fType)
  }.validation

  def parseValueType(vTypeStr: String) = vTypeStr.toLowerCase match {
      case "double" => "DecimalType".success
      case "int"    => "IntegralType".success
      case "string" => "StringType".success
      case _        => s"Unknown value type '$vTypeStr'".failure
    }

  def parseFeatureType(fTypeStr: String, vType: String) = fTypeStr.toLowerCase match {
      case "continuous" => vType match {
        case "DecimalType"  => "Continuous".success
        case "IntegralType" => "Discrete".success
        case _              => s"Invalid value type 'vType' for continuous feature".failure
      }
      case "categorical" => vType match {
        case "IntegralType" => "Ordinal".success
        case "StringType"   => "Nominal".success
        case _              => s"Invalid value type 'vType' for categorical feature".failure
      }
      case _ => s"Unknown feature type '$fTypeStr'".failure
   }

  def toScala(sourceType: String)(metadata: Iterable[Metadata]) = s"""
import commbank.coppersmith._
import Feature._
import Type._

trait $sourceType

object ${sourceType}FeatureSet extends MetadataSet[$sourceType] {
${metadata.map{ case (ns, name, vType, fType, desc) =>
s"""  val ${camelCase(name)} = Metadata[$sourceType, Value.$vType](
      "$ns", "$name", "$desc", $fType
  )"""
}.mkString("\n\n")}

  def metadata = List(
      ${
    metadata.grouped(4).map(g => g.map(m => camelCase(m._2)).mkString(", ")).mkString(",\n      ")
  }
  )
}"""

  def camelCase(s: String) = (s.split('_').toList match {
    case h :: t => h :: t.flatMap(w => w.headOption.map(i => i.toUpper + w.tail))
    case _      => List()
  }).mkString
}
