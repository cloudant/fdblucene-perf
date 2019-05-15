package com.cloudant.fdblucene.benchmark;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.TopDocs;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BasicSearchBenchmark {

    @State(Scope.Benchmark)
    public static class FDBSearchBenchmark {

        protected FDBSearchSetup fdbSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchFDB")
        @GroupThreads(1)
        public long searchFDB() throws Exception {
            TopDocs tp = fdbSetup.searcher.search(fdbSetup.queryMaker.makeQuery(),
                    fdbSetup.topNDocs);
            return tp.totalHits.value;
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            fdbSetup = new FDBSearchSetup();
            fdbSetup.startFDBNetworking();
            fdbSetup.createReader();
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception {
            fdbSetup.teardown();
        }
    }

    @State(Scope.Benchmark)
    public static class NioSearchBenchmark {
        protected NIOSSearchSetup nioSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchNIOS")
        @GroupThreads(1)
        public long searchNIOS() throws Exception {
            TopDocs tp = nioSetup.searcher.search(nioSetup.queryMaker.makeQuery(),
                    nioSetup.topNDocs);
            return tp.totalHits.value;
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            nioSetup = new NIOSSearchSetup();
            nioSetup.setupNIOS();
            nioSetup.createReader();
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception {
            nioSetup.teardown();
        }
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(BasicSearchBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}
