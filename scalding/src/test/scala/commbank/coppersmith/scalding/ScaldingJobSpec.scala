package commbank.coppersmith.scalding

import org.joda.time.DateTime

import com.twitter.scalding.{Config, Execution, TypedPipe}

import org.apache.hadoop.fs.Path

import org.scalacheck.Arbitrary
import org.scalacheck.Gen.alphaStr
import org.scalacheck.Prop.forAll

import scalaz.syntax.std.list.ToListOpsFromList

import au.com.cba.omnia.maestro.api._, Maestro._
import au.com.cba.omnia.maestro.test.Records

import au.com.cba.omnia.thermometer.core.Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids.records
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import commbank.coppersmith._, Feature._, FeatureBuilderSource.fromFS, Type._, Value._
import Arbitraries._

import commbank.coppersmith.thrift.Eavt
import commbank.coppersmith.test.thrift.{Account, Customer}

import ScaldingJobSpec.{RegularFeatures, AggregationFeatures}

class ScaldingJobSpec extends ThermometerHiveSpec with Records { def is = s2"""
    Running a regluar feature set job
      writes all regular feature values $regularFeaturesJob ${tag("slow")}

    Running an aggregation feature set job
      writes all aggregation feature values $aggregationFeaturesJob ${tag("slow")}
  """

  {
    import java.util.logging.Logger
    // Suppress parquet logging (which wraps java.util.logging) in tests. Loading the parquet.Log
    // class forces its static initialiser block to be run prior to removing the root logger
    // handlers, which needs to be done to avoid the Log's default handler being re-added. Disabling
    // log output specific to the parquet logger would be more ideal, however, this has proven to be
    // non-trivial due to what appears to be a result of configuration via static initialisers and
    // the order in which the initialisers are called.
    // FIXME: This can be replaced by TestUtil.withoutLogging when slf4j is available in parquet
    // via https://github.com/apache/parquet-mr/pull/290
    Logger.getLogger(getClass.getName).info("Suppressing further java.util.logging log output")
    Class.forName("parquet.Log")
    val rootLogger = Logger.getLogger("")
    rootLogger.getHandlers.foreach(rootLogger.removeHandler)
  }

  def prepareData(
    custAccts:   CustomerAccounts,
    jobTime:     DateTime,
    hydroConfig: HydroSink.Config
  ): FeatureJobConfig[Account] = {

    val accounts = custAccts.cas.flatMap(_.as)
    case class PrepareConfig(config: Config) {
      val maestro = MaestroConfig(config, "account", "account", "account")
      val acctTable = maestro.hiveTable[Account]("account", "account_db", Some("account_db"))
    }

    val job: Execution[JobStatus] = for {
      conf  <- Execution.getConfig.map(PrepareConfig(_))
               // Clear data from previous tests
      _     <- Execution.fromHdfs(Hdfs.delete(path(s"$dir/user/account_db"), true))
      _     <- Execution.fromHdfs(Hdfs.delete(path(s"$dir/user/features_db"), true))
      count <- viewHive(conf.acctTable, TypedPipe.from(accounts))
      _     <- Execution.guard(count == accounts.size, s"$count != ${accounts.size}")
    } yield JobFinished

    executesSuccessfully(job, Map("hdfs-root" -> List(s"$dir/user")))

    val accountDataSource = HiveParquetSource[Account, Nothing](
      path(s"$dir/user/account_db"),
      ScaldingDataSource.Partitions.unpartitioned
    )

    new FeatureJobConfig[Account] {
      val featureContext = ExplicitGenerationTime(jobTime)
      val featureSource  = From[Account].bind(SourceBinder.from(accountDataSource))
      val featureSink    = HydroSink(hydroConfig)
    }
  }

  val eavtReader  = delimitedThermometerRecordReader[Eavt]('|', "\\N", implicitly[Decode[Eavt]])
  val defaultArgs = Map("hdfs-root" -> List(s"$dir/user"))
  val hydroConfig = HydroSink.Config("features_db", path(s"$dir/user/features_db"), "features")

  // Use alphaStr to avoid problems with serialising new lines and hydro field delimiters
  implicit val arbCustAccts: Arbitrary[CustomerAccounts] = arbCustomerAccounts(alphaStr)

  def regularFeaturesJob =
    forAll { (custAccts: CustomerAccounts, jobTime: DateTime) => {
      val cfg = prepareData(custAccts, jobTime, hydroConfig)
      val expected = RegularFeatures.expectedFeatureValues(custAccts, jobTime)

      withEnvironment(path(getClass.getResource("/").toString)) {
        executesSuccessfully(SimpleFeatureJob.generate((_: Config) => cfg, RegularFeatures), defaultArgs)
        facts(
          path(s"${hydroConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)

  def aggregationFeaturesJob =
    forAll { (custAccts: CustomerAccounts, jobTime: DateTime) => {
      val cfg = prepareData(custAccts, jobTime, hydroConfig)
      val expected = AggregationFeatures.expectedFeatureValues(custAccts, jobTime)

      withEnvironment(path(getClass.getResource("/").toString)) {
        executesSuccessfully(SimpleFeatureJob.generate((_: Config) => cfg, AggregationFeatures), defaultArgs)
        facts(
          path(s"${hydroConfig.hiveConfig.path}/*/*/*/*") ==> records(eavtReader, expected)
        )
      }
    }}.set(minTestsOk = 5)
}

object ScaldingJobSpec {
  object RegularFeatures extends FeatureSet[Account] with FeatureSetWithTime[Account] {
    val namespace          = "test.namespace"
    def entity(a: Account) = a.id

    val source  = From[Account]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select: FeatureSetBuilder[Account, Account]  = builder

    type AF = Feature[Account, Value]

    val balanceF: AF = select(_.balance).asFeature(Continuous, "balance", "test")
    val ageF: AF = select.map(_.age).collect {
      case Some(age) => age
    }.asFeature(Continuous, "age", "test")

    def features = List(balanceF, ageF)

    def expectedFeatureValues(custAccts: CustomerAccounts, time: DateTime) = {
      custAccts.cas.flatMap(_.as.flatMap(acct => {
        val values = List(
                  List(FeatureValue[Decimal] (acct.id, "balance", acct.balance)),
          acct.age.map(FeatureValue[Integral](acct.id, "age",     _)).toList
        ).flatten
        values.map(HydroSink.toEavt(_, time.getMillis))
      })).toList
    }
  }

  object AggregationFeatures extends AggregationFeatureSet[Account] {
    val namespace          = "test.namespace"
    def entity(a: Account) = a.customerId

    val source  = From[Account]()
    val builder = source.featureSetBuilder(namespace, entity)
    val select: FeatureSetBuilder[Account, Account]  = builder

    type AAF = AggregationFeature[Account, Account, _, Value]

    val sizeF: AAF = select(size)          .asFeature(Continuous, "size", "test")
    val minF:  AAF = select(min(_.balance)).asFeature(Continuous, "min",  "test")

    import com.twitter.algebird.Aggregator

    val collectF: AggregationFeature[Account, Int, _, Value] =
      builder.map(_.age).collect {
        case Some(age) => age
      }.select(Aggregator.fromMonoid[Int]).asFeature(Continuous, "collect", "test")

    // Make sure reflection code in SimpleFeatureJobOps.Unjoiner works with empty source.
    // Note that it will never be in the expectedFeatureValues.
    val knownEmptyF: AggregationFeature[Account, Int, _, Value] =
      builder.map(_.age).collect {
        case Some(age) if false => age
      }.select(Aggregator.fromMonoid[Int]).asFeature(Continuous, "knownEmpty", "test")

    def aggregationFeatures = List(sizeF, minF, knownEmptyF, collectF)

    def expectedFeatureValues(custAccts: CustomerAccounts, time: DateTime) = {
      custAccts.cas.flatMap(cag => {
        val size    = cag.as.size
        val min     = cag.as.map(_.balance).min
        val ages    = cag.as.map(_.age).collect { case Some(age) => age }
        val collect = ages.toNel.map(_.list.sum)
        val values  = List(
                 List(FeatureValue[Integral](cag.c.id, "size",    size)),
                 List(FeatureValue[Decimal] (cag.c.id, "min",     min)),
          collect.map(FeatureValue[Integral](cag.c.id, "collect", _)).toList
        ).flatten
        values.map(HydroSink.toEavt(_, time.getMillis))
      }).toList
    }
  }
}
