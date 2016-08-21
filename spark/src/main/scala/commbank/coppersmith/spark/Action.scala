package commbank.coppersmith.spark

//Cheap and nasty effect monad for spark. May be wrong abstraction but helps port
//code

import org.apache.spark.sql.SparkSession
import scalaz.{Monad, Zip}

import org.apache.hadoop.conf.{Configuration => HadoopConfig}

import commonImports._

class Action[+A] private (val f: SparkSession => A)

object Action {
  def pure[A](a: => A): Action[A] = new Action(_ => a)
  def run[A](action: Action[A])(implicit session: SparkSession): A = action.f(session)

  val getSpark: Action[SparkSession] = new Action ({ spark => spark })
  def jobFailure(code: Int) = pure(throw new JobFailureException(code))


  implicit val actionInstance = new Monad[Action] with Zip [Action] {
    def point[A](a: => A) = Action.pure(a)
    def bind[A, B](fa: Action[A])(f: A => Action[B]) = new Action ({ spark =>
      println("HERE")
      //TODO deal reasonably with failure
      val a = fa.f(spark)
      val fb = f(a)
      fb.f(spark)
    })

  def zip[A, B](a: => Action[A], b: => Action[B]) = new Action ({ spark =>
    (a.f(spark), b.f(spark))
  })
}
  /** Changes from HDFS context to Execution context. */
 def fromHdfs[T](hdfs: Hdfs[T]): Action[T] = {
   import Action.actionInstance.monadSyntax._
   for {
     conf <- hadoopConf
   } yield hdfs.run(conf).getOrElse(throw new RuntimeException("DEATH"))
  }


 val hadoopConf: Action[HadoopConfig] = new Action({ spark =>
   val sparkConf = spark.sparkContext.getConf
   val hadoopConf = new HadoopConfig

   //probably getExecutorEnv, but have no idea really. Probably pretty important
   //to get this right
   sparkConf.getExecutorEnv.foreach { case (k,v) =>
     hadoopConf.set(k, v)
   }
   hadoopConf
  })
}

case class JobFailureException(exitCode: Int) extends Exception(s"Job failure with exit code $exitCode")
