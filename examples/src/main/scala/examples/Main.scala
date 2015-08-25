package commbank.coppersmith.examples

import org.apache.hadoop

import com.twitter.scalding

/**
  * To run a job, provide class name and args "input", "output", and "error". Eg:
  *
  *     hadoop jar examples-assembly.jar
  *       au.com.cba.omnia.dataproducts.features.examples.ScaldingFieldsCsvJob \
  *       --hdfs \
  *       --input /user/name/wide1 \
  *       --output /user/name/ScaldingFieldsCsv_output \
  *       --error /user/name/ScaldingFieldsCsv_error
  */
object Main {
  def main(args: Array[String]): Unit = {
    System.exit(hadoop.util.ToolRunner.run(new hadoop.conf.Configuration, new scalding.Tool, args))
  }
}
