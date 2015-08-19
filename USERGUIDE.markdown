Coppersmith User Guide
======================

This is a guide to using the Coppersmith library to generate features.
It is aimed at programmers and data engineers.
The **Basics** section introduces the fundamental concepts
and should be read in full.
The **Advanced** section serves as a reference
to additional features of the library.


Basics
------

Coppersmith separates two key concerns of feature generation:
the formula or calculation itself,
and the process of executing this calculation
on some source data.


### The `Feature` class

A individual feature is represented by the `Feature[S, V]` class.
Its two type parameters are
the *source* (or input) type
and the *value* (or output) type.
You can think of it as a function from `S` to `V`:
- The source type is typically a thrift struct,
  describing the schema of a raw table
  or (more likely) an "Analytical Record".
- The value type is one of `Integral`, `Decimal`, or `Str`.

A feature must also define some metadata, including:
- a *namespace*,
- a feature *name*,
- and a feature *type* (`Categorial` or `Continuous`).

Below is an example of a feature defined by extending the `Feature` class:

    // NOTE: This example is for pedagogical purposes only; it is not the
    // recommended approach. Consider using the convenience methods of the
    // FeatureSet class (see below) instead of extending Feature directly.

    import org.joda.time.DateTime

    import au.com.cba.omnia.dataproducts.features.{Feature, FeatureMetadata, FeatureValue}
    import Feature.Type._, Feature.Value._
    import au.com.cba.omnia.dataproducts.features.example.thrift.Customer

    object customerBirthYear extends Feature[Customer, Integral](
      FeatureMetadata(namespace  ="userguide.examples",
                      name       ="CUST_BIRTHYEAR",
                      featureType=Continuous)
    ) {
      def generate(cust: Customer) = Some(FeatureValue(
        entity=cust.id,
        name="CUST_BIRTHYEAR",
        value=DateTime.parse(cust.dob).getYear,
        time=DateTime.parse(cust.effectiveDate).getMillis
      ))
    }


### The `FeatureSet` class

`FeatureSet[S]` is a group of features
derived from the same source record type `S`.
There are several specialised subtypes
such as `BasicFeatureSet`, `PivotFeatureSet`, and `AggregationFeatureSet`.
Extending one of these classes is the recommended way of creating features,
since it avoids duplicating the definition of namespace, entity, etc.
Even if you only want to create one feature,
it is still better to start with a `FeatureSet`,
and use the convenience methods such as `basicFeature`
to express the feature logic.

A feature set must define a property called `features`
which is simply an iterable collection of `Feature` objects.
The recommended pattern is to define each individual feature as a `val`,
using a convenience method such as `basicFeature`,
and then set the `features` property to a `List` of these vals.
Typically the compiler can infer most of the types,
including the feature value type
(for example `Int` and `Long` are promoted to the `Integral` type).
Type inference allows for a cleaner feature definition.

Here is an example using `BasicFeatureSet`
(which provides the method `basicFeature`).
For details of the other classes available, refer to the **Advanced** section.

    import org.joda.time.DateTime

    import au.com.cba.omnia.dataproducts.features.{BasicFeatureSet, Feature}
    import Feature.Type._, Feature.Value._
    import au.com.cba.omnia.dataproducts.features.example.thrift.Customer

    object customerFeatures extends BasicFeatureSet[Customer] {
      val namespace              = "userguide.examples"
      def entity(cust: Customer) = cust.id
      def time(cust: Customer)   = DateTime.parse(cust.effectiveDate).getMillis

      val customerBirthYear = basicFeature[Integral]("CUST_BIRTHYEAR", Continuous, {
        (cust) => DateTime.parse(cust.dob).getYear
      })
    }


### Execution (aka Lifting)

(to be written)
