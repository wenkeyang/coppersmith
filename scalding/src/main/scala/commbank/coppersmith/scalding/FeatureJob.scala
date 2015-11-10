package commbank.coppersmith.scalding

import com.twitter.algebird.Aggregator

import com.twitter.scalding.typed._
import com.twitter.scalding.{Config, Execution}

import au.com.cba.omnia.maestro.api._

import commbank.coppersmith.Feature._
import commbank.coppersmith._

trait FeatureJobConfig[S] {
  def featureSource:  BoundFeatureSource[S, TypedPipe]
  def featureSink:    FeatureSink
  def featureContext: FeatureContext
}

abstract class SimpleFeatureJob extends MaestroJob with SimpleFeatureJobOps {
  val attemptsExceeded = Execution.from(JobNeverReady)
}

object SimpleFeatureJob extends SimpleFeatureJobOps

trait SimpleFeatureJobOps {
  def generate[S](cfg:      Config => FeatureJobConfig[S],
                  features: FeatureSetWithTime[S]): Execution[JobStatus] =
    generate[S](cfg, generateOneToMany(features)_)

  def generate[S](cfg:      Config => FeatureJobConfig[S],
                  features: AggregationFeatureSet[S]): Execution[JobStatus] =
    generate[S](cfg, generateAggregate(features)_)

  def generate[S](
    cfg:       Config => FeatureJobConfig[S],
    transform: (TypedPipe[S], FeatureContext) => TypedPipe[(FeatureValue[_], Time)]
  ): Execution[JobStatus] = {
    for {
      conf   <- Execution.getConfig.map(cfg)
      source  = conf.featureSource
      input   = source.load
      values  = transform(input, conf.featureContext)
      _      <- conf.featureSink.write(values)
    } yield JobFinished
  }

  private def generateOneToMany[S](
    features: FeatureSetWithTime[S]
  )(input: TypedPipe[S], ctx: FeatureContext): TypedPipe[(FeatureValue[_], Time)] = {
    input.flatMap { s =>
      val time = features.time(s, ctx)
      features.generate(s).map(fv => (fv, time))
    }
  }

  private def generateAggregate[S](
    features: AggregationFeatureSet[S]
  )(input: TypedPipe[S], ctx: FeatureContext): TypedPipe[(FeatureValue[_], Time)] = {
    val grouped: Grouped[EntityId, S] = input.groupBy(s => features.entity(s))
    val (joinedAggregator, unjoiner) = join(features.aggregationFeatures.toList.map(serialisable(_)))
    grouped.aggregate(joinedAggregator).toTypedPipe.flatMap { case (e, v) =>
      unjoiner.values(e, v).map((_, ctx.generationTime.getMillis))
    }
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
    aggregator: Aggregator[SV, B, V],
    view: PartialFunction[S, SV]
  ): Aggregator[S, Option[B], Option[Value]] = {
    import com.twitter.algebird.MonoidAggregator
    val lifted: MonoidAggregator[SV, Option[B], Option[V]] = aggregator.lift
    new MonoidAggregator[S, Option[B], Option[Value]] {
      def prepare(s: S) = if (view.isDefinedAt(s)) lifted.prepare(view(s)) else None
      def monoid = lifted.monoid
      def present(b: Option[B]) = lifted.present(b).map(v => v: Value)
    }
  }

  // Note: Could probably avoid reflection in join() and pattern matching on types in
  // NextUnjoiner.values() by changing AggregationFeatureSet.aggregationFeatures to be an HList

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
        (a.aggregator.join(agg).asInstanceOf[Aggregator[S, _, _]], NextUnjoiner(a.name, remaining))
      }
      // Dummy aggregator for base case - value will always be ignored by LastUnjoiner
      case Nil => (Aggregator.const[Option[Value]](None), LastUnjoiner)
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
  sealed trait Unjoiner {
    def values(e: EntityId, a: Any): List[FeatureValue[Value]]
  }

  case class NextUnjoiner(name: Name, remaining: Unjoiner) extends Unjoiner {
    def values(e: EntityId, a: Any): List[FeatureValue[Value]] = a match {
      case (Some(v: Value), vs) => FeatureValue(e, name, v) :: remaining.values(e, vs)
      case (None, vs) => remaining.values(e, vs)
      // Will only occur if implemenation of values falls out of sync with join (from above).
      case _ => sys.error("Assumption failed: Wrong shape " + a)
    }
  }

  // Marker for recursion base case
  case object LastUnjoiner extends Unjoiner {
    def values(e: EntityId, a: Any): List[FeatureValue[Value]] = List()
  }
}
