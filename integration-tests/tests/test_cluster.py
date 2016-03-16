import contextlib

import unittest
import json
import time

import tests.helpers as h


class TestCluster(unittest.TestCase):
    # @h.timeout(60)
    def test_distributed_aggregation(self):
        features = [
            'com.spotify.heroic.distributed_aggregations'
        ]

        q = "sum(size=10ms) from points(0, 100) where $key = test"

        with self.three_node_cluster(2, q, features=features) as result:
            self.assertEquals(1, len(result['result']))

            self.assertEquals(
                result['result'][0]['values'],
                [[0, 4.0], [10, 4.0], [20, 2.0]]
            )

    def test_sharded_query(self):
        q = "sum(size=10ms) from points(0, 100) where $key = test"

        with self.three_node_cluster(2, q) as result:
            self.assertEquals(2, len(result['result']))

            self.assertEquals(
                [r['values'] for r in result['result']],
                [[[0, 1.0], [20, 2.0]], [[0, 3.0], [10, 4.0]]]
            )

    def test_complex_query(self):
        features = [
            'com.spotify.heroic.distributed_aggregations'
        ]

        q = (
            "let $a = sum(size=10ms) from points(0, 100) where $key = test;" +
            "$a - $a - $a - $a"
        )

        with self.three_node_cluster(2, q, features=features) as result:
            self.assertEquals(1, len(result['result']))

            self.assertEquals(
                result['result'][0]['values'],
                [[0, -8.0], [10, -8.0], [20, -4.0]]
            )

    @contextlib.contextmanager
    def three_node_cluster(self, groups, query, features=[]):
        with h.managed_cluster(3, features=features) as nodes:
            a = nodes[0]
            b = nodes[1]

            # write some data into each shard
            a.write({"key": "test", "tags": {"foo": "bar", "bar": "a"}},
                    {"type": "points",
                     "data": [[0, 1], [20, 2]]})

            b.write({"key": "test", "tags": {"foo": "bar", "bar": "b"}},
                    {"type": "points",
                     "data": [[0, 3], [10, 4]]})

            # query for the data (without aggregation)
            result = a.query_metrics(
                {"query": query})

            self.assertEquals(200, result.status_code)

            result = result.json()

            self.assertEquals(0, len(result['errors']))

            result['result'] = sorted(result['result'],
                                      key=lambda r: r['tags'])

            yield result
