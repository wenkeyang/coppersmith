import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api.JobFinished

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

object MillionthCustomerSpec extends ThermometerHiveSpec { def is = s2"""
  MillionthCustomerFeaturesJob must return expected values  $test  ${tag("slow")}
"""
  def test = {
    executesSuccessfully(MillionthCustomerFeaturesJob.job) must_== JobFinished
    expectations { context =>
      context.lines(new Path(s"$dir/user/dev/view/warehouse/features/customers/year=2015/month=08/day=29/*")).toSet must_== Set(
        "1000000|CUST_MILLIONTH|Y|2015-08-29",
        "2000000|CUST_MILLIONTH|Y|2015-08-29",
        "3000000|CUST_MILLIONTH|Y|2015-08-29",
        "4000000|CUST_MILLIONTH|Y|2015-08-29",
        "5000000|CUST_MILLIONTH|Y|2015-08-29",
        "6000000|CUST_MILLIONTH|Y|2015-08-29",
        "7000000|CUST_MILLIONTH|Y|2015-08-29",
        "8000000|CUST_MILLIONTH|Y|2015-08-29",
        "9000000|CUST_MILLIONTH|Y|2015-08-29",
        "10000000|CUST_MILLIONTH|Y|2015-08-29"
      )
    }
  }
}
