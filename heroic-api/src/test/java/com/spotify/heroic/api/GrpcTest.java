package com.spotify.heroic.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.proto.heroic.Heroic;
import com.spotify.heroic.proto.heroic.QueryMetrics;
import com.spotify.heroic.proto.heroic.Range;
import com.spotify.heroic.proto.heroic.Unit;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

public class GrpcTest {
    @Test(timeout = 1000L)
    public void testGrpc() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);

        final Heroic.ServerStub stub = new Heroic.ServerStub() {
            @Override
            public void queryMetrics(
                final QueryMetrics request, final StreamObserver<QueryMetrics.Response> observer
            ) {
                final QueryMetrics.Response response = new QueryMetrics.Response.Builder()
                    .commonTags(ImmutableMap.of())
                    .result(ImmutableList.of())
                    .range(new QueryMetrics.Response.Range(10000L, 20000L))
                    .queryId("query")
                    .limits(ImmutableList.of())
                    .cached(false)
                    .preAggregationSampleSize(0L)
                    .errors(ImmutableList.of())
                    .build();

                observer.onNext(response);
                observer.onCompleted();

                latch.countDown();
            }
        };

        final Server server = doServer(stub);
        final ManagedChannel channel = doClient(latch);

        latch.await();

        channel.shutdown();
        server.shutdown();
    }

    private Server doServer(final Heroic.ServerStub stub) throws Exception {
        final Server server = ServerBuilder.forPort(1234).addService(stub).build();
        server.start();
        return server;
    }

    private ManagedChannel doClient(final CountDownLatch latch) throws Exception {
        final ManagedChannel channel =
            ManagedChannelBuilder.forAddress("localhost", 1234).usePlaintext(true).build();

        final CountDownLatch connected = new CountDownLatch(1);

        channel.notifyWhenStateChanged(ConnectivityState.READY, connected::countDown);

        connected.await();

        final Heroic.ClientStub client = new Heroic.ClientStub(channel);

        final QueryMetrics query =
            new QueryMetrics.Builder().range(new Range.Relative(Unit.HOURS, 2)).build();

        client.queryMetrics(query, new StreamObserver<QueryMetrics.Response>() {
            @Override
            public void onNext(final QueryMetrics.Response response) {
                System.out.println(response);
            }

            @Override
            public void onError(final Throwable throwable) {
                throwable.printStackTrace(System.err);
            }

            @Override
            public void onCompleted() {
                System.out.println("done");
                latch.countDown();
            }
        });

        return channel;
    }
}
