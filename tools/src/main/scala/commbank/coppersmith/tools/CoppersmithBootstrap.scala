// Package deliberately commented so file can be run as a script
//package commbank.coppersmith.tools

import java.io.{File, FileInputStream, PrintStream}

import scala.io.Source

/*
 * Tool for generating feature stub scala code from an existing metadata file. Supports
 * both PSV and CSV (based on file name extension).
 *
 * Supports the following formats
 *
 *   namespace.name|valueType|featureType
 *   namespace.name|valueType|featureType|description
 *
 * where valueType is one of "double", "int" or "string" and featureType is one of
 * "continuous" or "categorical"
 *
 * Example command-line usage:
 *
 * scala CoppersmithBootstrap.scala --source-type Customer --file /path/to/metadata.psv --out CustomerFeatures.scala
 *
 * If --out is missing, result is printed to STDOUT.
 */
object CoppersmithBootstrap {
  def main(rawArgs: Array[String]): Unit = {
    val args       = parseArgs(rawArgs)
    val sourceType = args.getOrElse("source-type", sys.error("--source-type arg missing"))
    val inFile     = new File(args.getOrElse("file", sys.error("--file arg missing")))
    val outFile    = args.get("out").map(new File(_))

    val sep = inFile.getName.reverse.takeWhile(_ != '.').reverse.toLowerCase match {
      case "csv" => ','
      case "psv" => '|'
      case x     => sys.error(s"Unsupported file extension '$x'")
    }

    lazy val out = outFile.map(new PrintStream(_))
    try {
      run(sourceType, inFile, sep, out).fold(
        e => {
          System.err.println(e)
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

  def run(sourceType: String, inFile: File, sep: Char, out: => Option[PrintStream]) = {
    if (!inFile.exists) {
      Left(s"File not found: ${inFile.getAbsolutePath}")
    } else {
      val fileIn = new FileInputStream(inFile)
      val source = Source.fromInputStream(fileIn)
      try {
        bootstrapScala(sourceType, source, sep).right.map(out.getOrElse(System.out).println(_))
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

  def bootstrapScala(sourceType: String, metadata: Source, sep: Char): Either[String, String] = {
    val featureMetadata: Either[String, List[Metadata]] =
      metadata.getLines.map(_.trim).zipWithIndex.filterNot { case (l, _) =>
        l.isEmpty || l.startsWith("#")
      }.toList.foldLeft[Either[String, List[Metadata]]](Right(List())) {
        case (r, (line, idx)) => r match {
          case e@Left(_) => e
          case Right(ms) => parseMetadata(line, sep).fold(
            e => Left(s"Error at line ${idx + 1}: $e"),
            m => Right(m :: ms)
          )
        }
      }

    featureMetadata.right.map(toScala(sourceType))
  }

  def parseMetadata(s: String, sep: Char): Either[String, Metadata] = {
    val values = s.split(sep)
    val parts: Either[String, (String, String, String)] = values.toList.take(3) match {
      case List(qName, vTypeStr, fTypeStr) => Right((qName, vTypeStr, fTypeStr))
      case _                               => Left(s"Could not parse separated values from '$s'")
    }

    parts.right.flatMap { case (qName, vTypeStr, fTypeStr) =>
      parseName(qName).right.flatMap { case (namespace, name) =>
        parseTypes(vTypeStr, fTypeStr).right.map { case (vType, fType) => {
          val desc = if (values.size == 4) values(3) else s"Description for $name"
          (namespace, name, vType, fType, desc)
        }}
      }
    }
  }

  def parseName(qName: String): Either[String, (String, String)] = qName.split('.') match {
    case Array(ns, name) => Right((ns, name))
    case _               => Left(s"Could not parse name from '$qName'")
  }

  def parseTypes(vTypeStr: String, fTypeStr: String) = {
    for {
      vType <- parseValueType(vTypeStr).right
      fType <- parseFeatureType(fTypeStr, vType).right
    } yield (vType, fType)
  }

  def parseValueType(vTypeStr: String) = vTypeStr.toLowerCase match {
    case "double" => Right("Decimal")
    case "int"    => Right("Integral")
    case "string" => Right("Str")
    case _        => Left(s"Unknown value type '$vTypeStr'")
  }

  def parseFeatureType(fTypeStr: String, vType: String) = fTypeStr.toLowerCase match {
    case "continuous" => vType match {
      case "Decimal"  => Right("Continuous")
      case "Integral" => Right("Discrete")
      case _          => Left(s"Invalid value type '$vType' for continuous feature")
    }
    case "categorical" => vType match {
      case "Integral" => Right("Ordinal")
      case "Str"      => Right("Nominal")
      case _          => Left(s"Invalid value type '$vType' for categorical feature")
    }
    case _ => Left(s"Unknown feature type '$fTypeStr'")
  }

  def toScala(sourceType: String)(metadata: Iterable[Metadata]) = s"""
import commbank.coppersmith.api._
import commbank.coppersmith.MetadataOutput
trait $sourceType

object ${sourceType}FeatureSet extends MetadataSet[$sourceType] {
${metadata.map{ case (ns, name, vType, fType, desc) =>
    s"""  val ${camelCase(name)} = FeatureStub[$sourceType, $vType].asFeatureMetadata($fType, "$ns", "$name", "$desc")
"""
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
