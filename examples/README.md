Examples of Usage
=================

This subproject contains examples based on a 431-column raw input source. The
schema is anonymised, with columns named `f1`, `f2`, `f3`, ..., `f431`.

**Do not commit any sample data to this repo**. Sample data (starting with a 12G
csv file) lives on the cluster.

The example scenario involves three first-order features `g1`, `g2`, and `g3`
(calculated from the raw attributes alone), and one second-order feature `h1`
which depends on both raw attributes and first-order features.

An additional "twist" is that the features are only generated for a subset of
the data, filtered based on the value of `f2`.
