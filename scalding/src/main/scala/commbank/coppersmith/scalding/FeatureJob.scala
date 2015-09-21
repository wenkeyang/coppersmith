package commbank.coppersmith.scalding

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

abstract class SimpleFeatureJob extends MaestroJob {
  val attemptsExceeded = Execution.from(JobNeverReady)

  def generate[S](cfg:      Config => FeatureJobConfig[S],
                  features: FeatureSetWithTime[S]): Execution[JobStatus] =
    generate[S](cfg, generateOneToMany(features)_)

  def generate[S](cfg:      Config => FeatureJobConfig[S],
                  features: AggregationFeatureSet[S]): Execution[JobStatus] =
    generate[S](cfg, generateAggregate(features)_)

  def generate[S](
    cfg:      Config => FeatureJobConfig[S],
    transform: (TypedPipe[S], FeatureContext) => TypedPipe[(FeatureValue[_], Time)]
  ): Execution[JobStatus] = {
    for {
      conf     <- Execution.getConfig.map(cfg)
      source    = conf.featureSource
      input     = source.load
      values    = transform(input, conf.featureContext)
      _        <- conf.featureSink.write(values)
    } yield JobFinished
  }

  private def generateOneToMany[S](features: FeatureSetWithTime[S])
                                  (input: TypedPipe[S], ctx: FeatureContext): TypedPipe[(FeatureValue[_], Time)] =
    input.flatMap { s =>
      val time = features.time(s, ctx)
      features.generate(s).map(fv => (fv, time))
    }

  // Should be able to take advantage of shapless' tuple support in combination with Aggregator.join
  // in order to run the aggregators in one pass over the input. Need to consider that features may
  // have different filter conditions though.
  // TODO: Where unable to join aggregators, might be possible to run in parallel instead
  private def generateAggregate[S](
    features: AggregationFeatureSet[S]
  )(input: TypedPipe[S], ctx: FeatureContext): TypedPipe[(FeatureValue[_], Time)] = {
/*
<<<<<<< HEAD
    val grouped: Grouped[EntityId, S] = input.groupBy(s => features.entity(s))
    features.aggregationFeatures.map(feature => {
      val name = feature.name
      // TODO: Unnecessarily traverses grouped when feature.where is None, however, there doesn't
      // appear to be a common supertype of Grouped and UnsortedGrouped with aggregate. One option
      // might be Either[Grouped, UnsortedGrouped].fold(_.aggregate(...), _.aggregate(...)).merge
      val filtered = grouped.filter { case (_, s) => feature.where.map(_(s)).getOrElse(true) }
      filtered.aggregate(feature.aggregator).toTypedPipe.map { case (e, v) =>
        (FeatureValue(e, name, v), ctx.generationTime.getMillis)
      }
    }).foldLeft(TypedPipe.from(List[(FeatureValue[_], Time)]()))(_ ++ _)
=======
*/
    val grouped: Grouped[EntityId, S] = input.groupBy(s => features.entity(s))
    features.aggregationFeatures.map(
      aggregate(grouped, _, ctx)
    ).foldLeft(TypedPipe.from(List[(FeatureValue[_], Time)]()))(_ ++ _)
  }

  private def aggregate[S, SV, V <: Value](
    grouped: Grouped[EntityId, S],
    feature: AggregationFeature[S, SV, _, V],
    ctx:     FeatureContext
  ) = {
    val name = feature.name
    // TODO: Unnecessarily traverses grouped when feature.where is None, however, there doesn't
    // appear to be a common supertype of Grouped and UnsortedGrouped with aggregate. One option
    // might be Either[Grouped, UnsortedGrouped].fold(_.aggregate(...), _.aggregate(...)).merge
    val view = grouped.toTypedPipe.collect {
      case (e, s) if feature.view.isDefinedAt(s) => (e, feature.view(s))
    }.group.filter { case (_, s) => feature.where.forall(_(s))}
    view.aggregate(feature.aggregator).toTypedPipe.map { case (e, v) =>
      (FeatureValue(e, name, v), ctx.generationTime.getMillis)
    }
  }
}
