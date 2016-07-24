# Metadata JSON

This document describes the different JSON representations of the generated metadata.
As the metadata gets richer, new versions will be added but these are expected to
be less "agile" than the rest of the coppersmith API, in the sense that we will
support older versions for a while and we will avoid changing the format without
good reason.

---

## Version 0

This is the legacy version ad-hoc version that was output by coppersmith prior
to the design of Version 1. We support it because we have published it for a while
but we recommend moving to Version 1.

Essentially, entire document is a list of metadata for individual features.

Example:

```json
[
    {
      "namespace":"movies",
      "name":"feature_name",
      "description":"Feature description",
      "source":"scala.Tuple2[java.lang.String,scala.collection.Iterable[scala.Tuple2[commbank.coppersmith.thrift.Movie,scala.Long]]]",
      "typesConform":true,
      "valueType":"integral",
      "featureType":"continuous"
    },
    {
      "namespace":"movies",
      "name":"feature_name2",
      "description":"Feature description 2",
      "source":"commbank.coppersmith.thrift.Movie",
      "typesConform":true,
      "valueType":"integral",
      "featureType":"continuous",
      "range": {
        "min":"1",
        "max":"10"
      }
     },
     {
       "namespace":"movies",
       "name":"feature_name3",
       "description":"Feature description 3",
       "source":"commbank.coppersmith.thrift.Movie",
       "typesConform":true,
       "valueType":"string",
       "featureType":"nominal",
       "range": ["horror", "comedy"]
    }
]
```

The fields correspond to the following:

| Field        | Description                                            
|--------------|--------------------------------------------------------
| namespace    | The namespace the feature belongs to                   
| name         | The name of the feature                                
| description  | Feature description (intended to be consumed by humans)
| source       | The table or joined tables the data came from. Currently scala class names, likely to change in future versions
| valueType    | The type of the feature value. Currently integral, decimal, string, date or time
| featureType  | The statistical type of the feature. Continuous, discrete, nominal, ordinal or instant
| typesConform | Whether the *valueType* and *featureType* are compatible
| range        | Optional. The range of values. See description below


### Ranges

Numeric ranges are represented objects containing `max` and `min` fields. The field
values are strings, in order to capture a broader range of representable values.
Specifically, `Long` values are not all exactly representable in JSON, but we want
to express them exactly. Example:

```
{min: "1", max:"2000"}
```

Set ranges are represented as JSON arrays of allowed values. Example:
```
["horror", "comedy"]
```

---

## Version 1

Version 1 groups features by their feature set and provides the names of sources in
a list, rather than having to infer them from the scala type. A version 1 document also
has an `version: 1` attribute in the top-level object. All future versions will
have a version identifier.

Example:

```json
{
    "version": 1,
    "featureSets": [
        {
            "featureSet": "movie-features",
            "features": [
                  {
                      "namespace":"movies",
                      "name":"COMEDY_MOVIE_AVG_RATING",
                      "description":"Average rating for comedy movies",
                      "sourceType":"scala.Tuple2[java.lang.String,scala.collection.Iterable[scala.Tuple2[commbank.coppersmith.thrift.Movie,commbank.coppersmith.thrift.Director]]]",
                      "typesConform":true,
                      "valueType":"integral",
                      "featureType":"continuous"
                    }
            ]
        }
    ]
}
```
