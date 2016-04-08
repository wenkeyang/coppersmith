import sbt._
import scala.io.Source

object FeatureJobGenerator {
  def gen(files: Seq[File]): Seq[(String, String)] = {
    constructors(files) map { f => f.name -> f.construct }
  }

  class FeatureJobConstructor(val name: String, typeParams: String) {
    val delims = Map("Rating" -> """\t""")
    val splitTypes = typeParams.replaceAll("[()]", "").split(", ").toList

    def dataSourceString(collectionName: String, typeParam: String, delim: Option[String]) = {
      val delimString = (delim map (d => s""", \"$d\"""")).getOrElse("")
      f"val $collectionName%-14s = HiveTextSource[$typeParam, Nothing]" +
        f"""(new Path(\"data/$collectionName\"), partitions$delimString)"""
    }

    def removeOption(t: String) =
      t.replace("Option[", "").replace("]", "")

    def toLowerPlural(s: String) =
      s.toLowerCase + "s"

    val collectionNames: List[String] = splitTypes map (removeOption _ andThen toLowerPlural)

    def tableName: String = collectionNames match {
      case n :: _ => n
      case _ => ""
    }

    val thriftImport = {
      val importTypes = splitTypes map removeOption
      importTypes match {
        case List(single) => single
        case imports       => imports.mkString("{", ", ", "}")
      }
    }

    val dataSources = {
      (for {
        t <- splitTypes map removeOption
      } yield dataSourceString(toLowerPlural(t), t, delims.get(t))).mkString("\n  ")
    }

    val binding = {
      val dataSources = collectionNames.mkString(", ")
      if (splitTypes.length == 1) s"from($dataSources)"
      else if (splitTypes(1).contains("Option[")) s"leftJoin($dataSources)"
      else s"join($dataSources)"
    }

    val construct: String = {
      s"""|package commbank.coppersmith.examples.userguide
          |
          |import org.apache.hadoop.fs.Path
          |
          |import com.twitter.scalding.Config
          |
          |import org.joda.time.DateTime
          |
          |import au.com.cba.omnia.maestro.api.Maestro.DerivedDecode
          |
          |import commbank.coppersmith.api._, scalding._
          |import commbank.coppersmith.examples.thrift.$thriftImport
          |
          |case class ${name}Config(conf: Config) extends FeatureJobConfig[$typeParams] {
          |  val partitions     = ScaldingDataSource.Partitions.unpartitioned
          |  $dataSources
          |
          |  val featureSource  = $name.source.bind($binding)
          |
          |  val featureContext = ExplicitGenerationTime(new DateTime(2015, 1, 1, 0, 0))
          |
          |  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "$tableName")
          |}
          |
          |object ${name}Job extends SimpleFeatureJob {
          |  def job = generate(${name}Config(_), $name)
          |}
          |""".stripMargin
    }
  }

  val featureSetPattern = """(\w+) extends \w*FeatureSet\w*\[((?:\([\w\,\s\[\]]+\))|(?:\w+))""".r.unanchored
  val jobPattern = """(\w+)Job extends \w+Job""".r.unanchored

  def existingJobs(files: Seq[File]): Seq[String] =
    for {
      file <- files
      line <- Source.fromFile(file).getLines()
      List(job) <- jobPattern.unapplySeq(line)
    } yield job

  def constructors(files: Seq[File]): Seq[FeatureJobConstructor] = {
    val existing = existingJobs(files)
    for {
      file <- files
      line <- Source.fromFile(file).getLines()
      List(name, typeParams) <- featureSetPattern.unapplySeq(line)
      if !existing.contains(name)
    } yield new FeatureJobConstructor(name, typeParams)
  }


}