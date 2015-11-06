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

  // Should be able to take advantage of shapless' tuple support in combination with Aggregator.join
  // in order to run the aggregators in one pass over the input. Need to consider that features may
  // have different filter conditions though.
  private def generateAggregate[S](
    features: AggregationFeatureSet[S]
  )(input: TypedPipe[S], ctx: FeatureContext): TypedPipe[(FeatureValue[_], Time)] = {
    val grouped: Grouped[EntityId, S] = input.groupBy(s => features.entity(s))
    features.aggregationFeatures.map(f =>
      aggregate(grouped, serialisable(f), ctx)
    ).foldLeft(TypedPipe.from(List[(FeatureValue[_], Time)]()))(_ ++ _)
  }

  private def aggregate[S, SV, V <: Value](
    grouped: Grouped[EntityId, S],
    feature: SerialisableAggregationFeature[S, SV, V],
    ctx:     FeatureContext
  ) = {
    val name = feature.name
    val aggregator = composeView(feature.aggregator, feature.view)
    grouped.aggregate(aggregator).toTypedPipe.collect { case (e, Some(v)) =>
      (FeatureValue(e, name, v), ctx.generationTime.getMillis)
    }
  }

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

  // Work-around for problem of TypeTag instances being tied to AggregationFeature and failing at
  // runtime due to serialisation issues. A more elegant fix would be to try to inline the above
  // aggregate method into generateAggregate, however, getting the types to line up is non-trivial.
  private def serialisable[S, SV, V <: Value](feature: AggregationFeature[S, SV, _, V]) =
    SerialisableAggregationFeature[S, SV, V](feature.name, feature.view, feature.aggregator)

  case class SerialisableAggregationFeature[S, SV, V <: Value](
    name:       Name,
    view:       PartialFunction[S, SV],
    aggregator: Aggregator[SV, _, V]
  )
}
