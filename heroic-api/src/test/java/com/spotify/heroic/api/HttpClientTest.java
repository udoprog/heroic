package com.spotify.heroic.api;

import com.spotify.heroic.proto.heroic.Filter;
import com.spotify.heroic.proto.heroic.Point;
import com.spotify.heroic.proto.heroic.QueryMetrics;
import com.spotify.heroic.proto.heroic.Range;
import com.spotify.heroic.proto.heroic.ResultGroup;
import com.spotify.heroic.proto.heroic.Unit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class HttpClientTest {
    @Test
    public void testHttpClient() {
        final String target = System.getenv("HEROIC_TARGET");

        if (target == null) {
            throw new IllegalStateException();
        }

        final Client client = HttpClient.builder(target).build();

        final List<Filter> and = new ArrayList<>();
        and.add(new Filter.Key("system"));
        and.add(new Filter.MatchTag("role", "heroicapi"));
        and.add(new Filter.MatchTag("what", "disk-used-percentage"));
        final Filter filter = new Filter.And(and);

        final QueryMetrics query = new QueryMetrics.Builder()
            .range(new Range.Relative(Unit.HOURS, 2))
            .filter(filter)
            .build();

        final CompletableFuture<QueryMetrics.Response> response = client.queryMetrics(query);

        final QueryMetrics.Response r = response.join();

        for (final ResultGroup group : r.getResult()) {
            if (group instanceof ResultGroup.Points) {
                final ResultGroup.Points points = (ResultGroup.Points) group;

                for (final Point p : points.getValues()) {
                    System.out.println(p);
                }
            }
        }
    }
}
