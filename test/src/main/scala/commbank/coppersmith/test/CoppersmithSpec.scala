package commbank.coppersmith
package test

import commbank.coppersmith.Feature.EntityId
import commbank.coppersmith.api._
import commbank.coppersmith.lift.generated.GeneratedMemoryLift
import lift.memory

trait CoppersmithSpecSupport {
  object joins extends GeneratedMemoryLift

  def feature[S, V <: Value](f: Feature[S,V]) = new SpecFeature(f)
  def feature[S, SV, U, V <: Value]
    (f: AggregationFeature[S, SV, U, V])
    (implicit featureSet: AggregationFeatureSet[S])= new SpecAggFeature(f)
}


class SpecFeature[S, +V <: Value](val f: Feature[S, V]) {
  def onData(data: List[S]) = new SpecFeatureWithData[S, V](f, data)
 }

class SpecAggFeature[S, SV, U, V <: Value](val f: AggregationFeature[S, SV, U, V])
                                          (implicit featureSet: AggregationFeatureSet[S]) {
  def onData(data: List[S]) = new SpecAggFeatureWithData[S, SV, U, V](f, data)
}

//TODO: would be nice to return underlying values, not wrappers
class SpecFeatureWithData[S, +V <: Value](val f: Feature[S, V], val data: List[S]) {
  def valueFor(e: EntityId): Option[V] = memory.lift(f)(data)
    .filter(_.entity == e)
    .headOption
    .map(_.value)

  def values: List[V] = memory.lift(f)(data).map(_.value)
}

//TODO: Ditto above
class SpecAggFeatureWithData[S, SV, U, V <: Value](val f: AggregationFeature[S, SV, U, V], val data: List[S])
                                                  (implicit featureSet: AggregationFeatureSet[S]) {
  def valueFor(e: EntityId) = {
    val groupedData = data.groupBy(featureSet.entity(_)).toList
    memory.lift(f.toFeature(featureSet.namespace))(groupedData)
      .filter(_.entity == e)
      .headOption
      .map(_.value)
  }

  def values: List[V] = {
    val groupedData = data.groupBy(featureSet.entity(_)).toList
    memory.lift(f.toFeature(featureSet.namespace))(groupedData).map(_.value)
  }
}