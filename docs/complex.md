# Complex Queries

Note: this feature is *work in progress*.

Complex queries are queries which are aggregated from the result of one or more
named queries.

Example.

```
let $a = average from points($now - 2d, $now - 1d);
let $b = average from points($now - 1d, $now);

abs($a - $b) by host where site = lon
```

This would translate to the following JSON.

```json
{
    "let": {
        "a": ...,
        "b": ...
    },
    "aggregation": {
        "type": "abs",
        "aggregation": {
            "type": "minus",
            "left": {
                "type": "reference",
                "id": "a"
            },
            "right": {
                "type": "reference",
                "id": "b"
            }
        }
    }
}
```
