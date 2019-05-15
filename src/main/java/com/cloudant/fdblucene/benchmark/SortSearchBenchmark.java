package com.cloudant.fdblucene.benchmark;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class SortSearchBenchmark {

    @State(Scope.Benchmark)
    public static class FDBSortSearchBenchmark {

        private FDBSearchSetup fdbSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchFDBBySort")
        @GroupThreads(1)
        public void searchFDBBySort(Blackhole blackhole) throws Exception {
            Sort sort = new Sort(SortField.FIELD_SCORE,
                    new SortField("sorteddocdate", Type.STRING));
            // we don't actually care about the number of hits
            blackhole.consume(fdbSetup.searcher.search(
                    fdbSetup.queryMaker.makeQuery(), fdbSetup.topNDocs, sort));
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            fdbSetup = new FDBSearchSetup();
            fdbSetup.setSearchType(BenchmarkUtil.SearchTypeEnum.BySort);
            fdbSetup.startFDBNetworking();
            fdbSetup.createReader();
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception {
            fdbSetup.teardown();
        }
    }

    @State(Scope.Benchmark)
    public static class NioSortSearchBenchmark {

        private NIOSSearchSetup nioSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchNIOSBySort")
        @GroupThreads(1)
        public void searchNIOSBySort(Blackhole blackhole) throws Exception {
            Sort sort = new Sort(SortField.FIELD_SCORE,
                    new SortField("sorteddocdate", Type.STRING));
            // we don't actually care about the number of hits
            blackhole.consume(nioSetup.searcher.search(
                    nioSetup.queryMaker.makeQuery(), nioSetup.topNDocs, sort));
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            nioSetup = new NIOSSearchSetup();
            nioSetup.setSearchType(BenchmarkUtil.SearchTypeEnum.BySort);
            nioSetup.setupNIOS();
            nioSetup.createReader();
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception {
            nioSetup.teardown();
        }
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(SortSearchBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}

