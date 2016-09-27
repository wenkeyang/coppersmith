Coppersmith - Feature Generation, as Functions
================================
[![Build Status](https://travis-ci.org/CommBank/coppersmith.svg?branch=master)](https://travis-ci.org/CommBank/coppersmith)
[![Gitter chat](https://badges.gitter.im/CommBank/coppersmith.png)](https://gitter.im/CommBank/coppersmith)


> 1. a person who makes artifacts from copper.
>
> 2. data is malleable; fold and hammer it into various shapes
>    that are more attractive to analysts and data scientists.

**coppersmith** is a library to enable the joining, aggregation, and synthesis
of "features", streams of facts about entities derived from "analytical
records".

This library was originally written by a squad within the Analytics &
Information group at [Commonwealth Bank](https://www.commbank.com.au/), looking
to improve the task of authoring and maintaining features for use in predictive
analytics and machine learning.

Our working hypothesis was that for all the complexity of the business domain
and the size of the data sets involved, fundamentally the logic used in feature
generation can be described as simple functions and those functions should be
able to be composed. The framework now called **coppersmith** grew out of our
efforts to improve the lives of feature authors.

Documentation
-------------

We have a richly detailed [user guide](USERGUIDE.markdown),
which we consider a good introduction to **coppersmith**. PR's to the user
guide as you become familiar with the library are especially encouraged!!!

There is also a [troubleshooting guide](TROUBLESHOOTING.markdown) available and
a [GitHub Pages site](http://commbank.github.io/coppersmith/) which provides
additional information including latest version and usage information.

Generated Code
--------------
Classes and objects from the `commbank.coppersmith.generated`,
`commbank.coppersmith.scalding.generated` and
`commbank.coppersmith.lift.generated` packages are generated at
build time with [`MultiwayJoinGenerator`](project/MultiwayJoinGenerator.scala).
The generated files can be found under the `target/scala-2.11/src_managed/main/`
directory of the `core`, `scalding` and `test` subprojects respectively.

Versioning
----------

The [change log](CHANGELOG.markdown) lists all backwards-incompatible changes to
the library (i.e. changes which might break existing client code).
Any such changes require bumping the second number in the version.

Contributing
------------
If you'd like to contribute to Coppersmith, please consult the
[contributing guide](CONTRIBUTING.markdown)

Contributors
------------

- Kristian Domagala [@dkristian](https://github.com/dkristian)
- Todd Owen [@toddmowen](https://github.com/toddmowen)
- Tin Pavlinic [@triggerNZ](https://github.com/triggerNZ)
- Ewan Vaughan [@ewanv](https://github.com/ewanv)
- Andrew Cowie [@afcowie](https://github.com/afcowie)
- Konstantinos Servis [@knservis](https://github.com/knservis)
- Afsal Thaj [@afsalthaj](https://github.com/afsalthaj)
- TianYi Zhu [@Fantoccini](https://github.com/Fantoccini)
- Michael Thomas [@Michaelt293](https://github.com/Michaelt293)
