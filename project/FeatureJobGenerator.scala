import sbt._
import scala.io.Source

object FeatureJobGenerator {
  def gen(files: Seq[File]): Seq[(String, String)] = {
    constructors(files) map { f => f.name -> f.construct }
  }

  private class FeatureJobConstructor(val name: String, setType: String, typeParams: String) {
    private val delims = Map("Rating" -> Some("""\t""")).withDefaultValue(None)
    private val splitTypes = typeParams.replaceAll("[()]", "").split(", ")

    private def dataSourceString(collectionName: String, typeParam: String, delim: Option[String]) = {
      val delimString = (delim map (d => s""", \"$d\"""")).getOrElse("")
      f"val $collectionName%-14s = HiveTextSource[$typeParam, Nothing]" +
        f"""(new Path(\"data/$collectionName\"), partitions$delimString)"""
    }

    private def removeOption(t: String) =
      t.replace("Option[", "").replace("]", "")

    private def toLowerPlural(s: String) =
      s.toLowerCase + "s"

    val collectionNames: Array[String] = splitTypes map (removeOption _ andThen toLowerPlural)

    val thriftImport = {
      val importTypes = splitTypes map removeOption
      if (importTypes.length == 1) importTypes.head
      else s"{${importTypes.mkString(", ")}}"
    }

    val dataSources = {
      (for {
        t <- splitTypes map removeOption
      } yield dataSourceString(toLowerPlural(t), t, delims(t))).mkString("\n  ")
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
          |  val featureSink    = EavtSink.configure("userguide", new Path("dev"), "${collectionNames.head}")
          |}
          |
          |object ${name}Job extends SimpleFeatureJob {
          |  def job = generate(${name}Config(_), $name)
          |}
          |""".stripMargin
    }
  }

  private val featureSetPattern = """(\w+) extends (\w*FeatureSet\w*)\[((?:\([\w\,\s\[\]]+\))|(?:\w+))""".r.unanchored
  private val jobPattern = """(\w+)Job extends \w+Job""".r.unanchored

  private def existingJobs(files: Seq[File]): Seq[String] =
    for {
      file <- files
      line <- Source.fromFile(file).getLines()
      List(job) <- jobPattern.unapplySeq(line)
    } yield job

  private def constructors(files: Seq[File]): Seq[FeatureJobConstructor] = {
    val existing = existingJobs(files)
    for {
      file <- files
      line <- Source.fromFile(file).getLines()
      List(name, setType, typeParams) <- featureSetPattern.unapplySeq(line)
      if !existing.contains(name)
    } yield new FeatureJobConstructor(name, setType, typeParams)
  }


}