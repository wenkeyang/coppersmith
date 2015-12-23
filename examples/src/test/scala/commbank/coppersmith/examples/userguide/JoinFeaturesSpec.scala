package commbank.coppersmith.examples.userguide

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api.JobFinished
import au.com.cba.omnia.maestro.api.Maestro.DerivedEncode
import au.com.cba.omnia.maestro.core.codec.Encode

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import org.scalacheck.Gen

import commbank.coppersmith.examples.thrift.{Customer, Account}

object JoinFeaturesSpec extends ThermometerHiveSpec { def is = s2"""
  JoinFeaturesJob must return expected values  $test  ${tag("slow")}
"""
  def test = {
    def cust(id: String, dob: String) = Gen.resultOf(Customer.apply _).sample.get.copy(id = id, dob = dob)
    writeRecords[Customer](s"$dir/user/data/customers/data.txt", Seq(
      cust("C1", "1972-04-12"),  // post-1970
      cust("C2", "1968-02-15")   // pre-1970
    ))

    def acct(cust: String, bal: Int) = Gen.resultOf(Account.apply _).sample.get.copy(customer = cust, balance = bal)
    writeRecords[Account](s"$dir/user/data/accounts/data.txt", Seq(
      acct("C1", 100),
      acct("C2", 200),
      acct("C2", 300)
    ))

    executesSuccessfully(JoinFeaturesJob.job) must_== JobFinished

    expectations { context =>
      context.lines(new Path(s"$dir/user/dev/view/warehouse/features/balances/year=2015/month=08/day=29/*")).toSet must_== Set(
        "C2|CUST_BORN_PRE1970_TOT_BALANCE|500|2015-08-29"
      )
    }
  }

  def writeRecords[T : Encode](path: String, records: Seq[T]): Unit = {
    val lines = records.map(t => Encode.encode("", t).mkString("|"))
    writeLines(path, lines)
  }

  def writeLines(path: String, lines: Seq[String]): Unit = {
    import java.io.{File, PrintWriter}

    val file = new File(path)
    file.getParentFile.mkdirs()
    val writer = new PrintWriter(file)
    try {
      lines.foreach { writer.println(_) }
    }
    finally {
      writer.close()
    }
  }
}
