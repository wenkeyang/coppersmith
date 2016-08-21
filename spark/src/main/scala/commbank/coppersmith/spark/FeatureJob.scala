//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith
package spark

import org.apache.hadoop.fs.Path

import com.twitter.algebird.{MonoidAggregator, Aggregator}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import Feature._

import SparkyMaestroJob._

import Action.actionInstance.monadSyntax._
import Action.actionInstance.zipSyntax._

import org.slf4j.{Logger, LoggerFactory}

trait FeatureJobConfig[S] {
  def featureSource:  BoundFeatureSource[S, RDD]
  def featureSink:    FeatureSink
  def featureContext: FeatureContext
}

/** Extend this class if you just want to generate features,
  * and have no custom requirements for how jobs are launched, monitored, configured, etc.
  *
  * If you have any special requirements, consider [[SimpleFeatureJobOps]] instead.
  */
abstract class SimpleFeatureJob extends SparkyMaestroJob with SimpleFeatureJobOps {
  val attemptsExceeded = Action.pure(JobNeverReady)
}

object SimpleFeatureJob extends SimpleFeatureJobOps

///Looks kinda like a maestro job but isn't. Paste happy, fakes unnecessary concepts. Needs deletion
trait SparkyMaestroJob {
  import SparkyMaestroJob._
  /** The logger to use for this application */
  def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def job: Action[JobStatus]

  def main(args: Array[String]) {
    implicit val spark = SparkSession.builder().getOrCreate() //TODO: read args and configure properly
    val status = try {
      Action.run(job)
    } catch {
      case ex: Exception => {
        logger.error("error running execution", ex)
        JobFailure
      }
    }
    System.exit(status.exitCode)
  }
}

object SparkyMaestroJob {
  sealed trait JobStatus {
    def exitCode: Int
  }

  /** The job succeeded and had work to do */
  case object JobFinished extends JobStatus { val exitCode = 0 }

  /** The job is not ready to run: it's pre-requisites are not available */
  case object JobNotReady extends JobStatus { val exitCode = 1 }

  /** The Job was never ready, and could not be retried any more */
  case object JobNeverReady extends JobStatus { val exitCode = 2 }

  /** The Job was not scheduled to run, and should not be retried */
  case object JobNotScheduled extends JobStatus { val exitCode = 3 }

  case object JobFailure extends JobStatus {val exitCode = 4}
}

/** Mix in this trait if you need more flexiblity than [[SimpleFeatureJob]] provides.
  *
  * For example, if your organisation already has a bespoke base class which jobs need to extend,
  * then you can mix in this trait to add support for generating coppersmith features.
  */
trait SimpleFeatureJobOps {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass())

  def generate[S](cfg:      SparkSession => FeatureJobConfig[S],
                  features: FeatureSet[S]): Action[JobStatus] =
    generate(FeatureSetExecutions(FeatureSetExecution(cfg, features)))

  def generate[S](cfg:      SparkSession => FeatureJobConfig[S],
                  features: AggregationFeatureSet[S]): Action[JobStatus] =
    generate(FeatureSetExecutions(FeatureSetExecution(cfg, features)))

  def generate(featureSetExecutions: FeatureSetExecutions): Action[JobStatus] = {
    for {
      paths  <- generateFeatures(featureSetExecutions)
      result <- FeatureSink.commit(paths)
      status <- result.fold(writeErrorFailure(_), _ => Action.pure(JobFinished))
    } yield status
  }

  // Run each outer group of executions in sequence, accumulating paths at each step
  private def generateFeatures(featureSetExecutions: FeatureSetExecutions): Action[Set[Path]] =
    featureSetExecutions.allExecutions.foldLeft(Action.pure(Set[Path]()))(
      (resultSoFar, executions) => resultSoFar.flatMap(paths =>
        generateFeaturesPar(executions).map(_ ++ paths)
      )
    )

  // Run executions in parallel (zip), combining the tupled sets of of paths at each step
  private def generateFeaturesPar(executions: List[FeatureSetExecution]): Action[Set[Path]] =
    executions.foldLeft(Action.pure(Set[Path]()))(
      (zippedSoFar, featureSetExecution) =>
        zippedSoFar.fzip(featureSetExecution.generate).map {
          case (accPaths, paths) => accPaths ++ paths
        }
    )

  import FeatureSink.{AlreadyCommitted, AttemptedWriteToCommitted, WriteError}
  def writeErrorFailure[T](e: WriteError): Action[T] = e match {
    case AlreadyCommitted(path) => {
      log.error(s"Tried to commit already committed path: '$path'")
      Action.jobFailure(-2)
    }
    case AttemptedWriteToCommitted(path) => {
      log.error(s"Tried to write to committed path: '$path'")
      Action.jobFailure(-3)
    }
  }
}

/**
  * Inner level of allExecutions are zipped to run in parallel. Groups of executions that
  * form outer level are run sequentially
  */
case class FeatureSetExecutions(allExecutions: List[List[FeatureSetExecution]]) {
  /** Add a new group of executions to run after all previous groups */
  def andThen(executions: FeatureSetExecution*) =
    FeatureSetExecutions(allExecutions :+ executions.toList)
}

object FeatureSetExecutions {
  def apply(executions: FeatureSetExecution*): FeatureSetExecutions =
    FeatureSetExecutions(List(executions.toList))
}

trait FeatureSetExecution {
  type Source

  def config: SparkSession => FeatureJobConfig[Source]

  def features: Either[FeatureSet[Source], AggregationFeatureSet[Source]]

  import FeatureSetExecution.{generateFeatures, generateOneToMany, generateAggregate}
  def generate(): Action[Set[Path]] = features.fold(
    regFeatures => generateFeatures[Source](config, generateOneToMany(regFeatures)_, regFeatures),
    aggFeatures => generateFeatures[Source](config, generateAggregate(aggFeatures)_, aggFeatures)
  )
}

object FeatureSetExecution {
  def apply[S](
    cfg: SparkSession => FeatureJobConfig[S],
    fs:  FeatureSet[S]
  ): FeatureSetExecution = new FeatureSetExecution {
    type Source = S
    def config = cfg
    def features = Left(fs)
  }
  def apply[S](
    cfg: SparkSession => FeatureJobConfig[S],
    fs:  AggregationFeatureSet[S]
  ): FeatureSetExecution = new FeatureSetExecution {
    type Source = S
    def config = cfg
    def features = Right(fs)
  }

  import SimpleFeatureJob.writeErrorFailure
  private def generateFeatures[S](
    cfg:         SparkSession => FeatureJobConfig[S],
    transform:   (RDD[S], FeatureContext) => RDD[(FeatureValue[Value], FeatureTime)],
    metadataSet: MetadataSet[Any]
  ): Action[Set[Path]] = {
    for {
      spark <- Action.getSpark
      conf   = cfg(spark)
      source  = conf.featureSource
      input   = source.load
      values  = transform(input, conf.featureContext)
      result <- conf.featureSink.write(values, metadataSet)
      paths  <- result.fold(writeErrorFailure(_), Action.pure(_))
    } yield paths
  }

  private def generateOneToMany[S](
    features: FeatureSet[S]
  )(input: RDD[S], ctx: FeatureContext): RDD[(FeatureValue[Value], FeatureTime)] = {
    input.flatMap { s =>
      val time = features.time(s, ctx)
      features.generate(s).map(fv => (fv, time))
    }
  }

  private def generateAggregate[S](
    features: AggregationFeatureSet[S]
  )(input: RDD[S], ctx: FeatureContext): RDD[(FeatureValue[Value], FeatureTime)] = {
    val grouped: RDD[(EntityId, S)] = input.keyBy(s => features.entity(s))
    val (joinedAggregator : MonoidAggregator[S, _, _], unjoiner: Unjoiner) =
      join(features.aggregationFeatures.toList.map(serialisable(_)))
???

    //  grouped.aggregate(joinedAggregator).flatMap { case (e, v) =>
    //   unjoiner.apply(e, v).map((_, ctx.generationTime.getMillis))
    // }
  }

  // Work-around for problem of TypeTag instances being tied to AggregationFeature and failing at
  // runtime due to serialisation issues. A more elegant fix would be to try to inline the above
  // aggregate method into generateAggregate, however, getting the types to line up is non-trivial.
  // Also takes care of applying the source view, resulting in aggregators from the same set working
  // on the same source type.
  private def serialisable[S, SV, V <: Value](
    feature: AggregationFeature[S, SV, _, V]
  ): SerialisableAggregationFeature[S] = {
    val aggregator = composeView(feature.aggregator, feature.view)
    SerialisableAggregationFeature[S](feature.name, aggregator)
  }

  case class SerialisableAggregationFeature[S](
    name:       Name,
    aggregator: Aggregator[S, _, Option[Value]]
  )

  def composeView[S, SV, B, V <: Value](
    aggregator: Aggregator[SV, B, Option[V]],
    view: PartialFunction[S, SV]
  ): Aggregator[S, Option[B], Option[Value]] = {
    import com.twitter.algebird.MonoidAggregator
    new MonoidAggregator[S, Option[B], Option[Value]] {
      def prepare(s: S) = view.lift(s).map(aggregator.prepare(_))
      def monoid = new com.twitter.algebird.OptionMonoid[B]()(aggregator.semigroup)
      def present(bOpt: Option[B]) = bOpt.flatMap(aggregator.present(_))
    }
  }

  // Note: Could probably avoid reflection in join() and pattern matching on types in
  // unjoiner() by changing AggregationFeatureSet.aggregationFeatures to be an HList

  type Unjoiner = (EntityId, Any) => List[FeatureValue[Value]]

  /*
   * Join (compose) aggregators of the form:
   *
   *   List(Agg[S, A1, Option[Value1]], Agg[S, A2, Option[Value2]], Agg[S, A3, Option[Value2]], ...)
   *
   * to a single aggregator of the form
   *
   *   Agg[S, (A1, (A2, (A3, ...))), (Option[Value1], (Option[Value2], (Option[Value3], ...)))]
   *
   * and return with an Unjoiner that can deconstruct the resulting values and associate them
   * back with their original feature name.
   */
  def join[S](features: List[SerialisableAggregationFeature[S]]): (Aggregator[S, _, _], Unjoiner) = {
    features match {
      case a :: as => {
        val (agg, remaining) = join(as)
        (a.aggregator.join(agg).asInstanceOf[Aggregator[S, _, _]], unjoiner(a.name, remaining))
      }
      // Dummy aggregator for base case - value will always be ignored by last unjoiner
      case Nil => (Aggregator.const[Option[Value]](None), (_, _) => List())
    }
  }

  /*
   * Takes values in the form of:
   *
   *    (Option[Value1], (Option[Value2], (Option[Value3], ...)))
   *
   * from the result of a set of joined aggregation features, and returns a list of
   * corresponding FeatureValue instances with the original feature name:
   *
   *   List(FeatureValue(e, name1, val1), FeatureValue(e, name2, val2), FeatureValue(e, name3, val3), ...)
   *
   * `None` values are filtered out of the returned list, as they represent input that that
   * is filtered completely out as a result of lifting the original aggregator and source
   * view PartialFunction into the Option MonoidAggregator.
   */
  def unjoiner(name: Name, remaining: Unjoiner)(e: EntityId, a: Any) = a match {
    case (Some(v: Value), vs) => FeatureValue(e, name, v) :: remaining.apply(e, vs)
    case (None, vs) => remaining(e, vs)
    // Will only occur if implemenation of values falls out of sync with join (from above).
    case _ => sys.error("Assumption failed: Wrong shape " + a)
  }
}
