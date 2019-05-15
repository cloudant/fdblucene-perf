package com.cloudant.fdblucene.benchmark;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;

import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class GroupSearchBenchmark {

    @State(Scope.Benchmark)
    public static class FDBGroupSearchBenchmark {

        private FDBSearchSetup fdbSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchFDBByGroup")
        @GroupThreads(1)
        public void searchFDBByGroup(Blackhole blackhole) throws Exception {
            Sort groupSort = new Sort(SortField.FIELD_SCORE,
                    new SortField("sorteddocdate", Type.STRING));
            FirstPassGroupingCollector c1 = new FirstPassGroupingCollector(
                    new TermGroupSelector("group100"), groupSort, fdbSetup.topNDocs);
            boolean cacheScores = true;
            double maxCacheRAMMB = 4.0;
            CachingCollector cachedCollector = CachingCollector.create(c1, cacheScores, maxCacheRAMMB);
            fdbSetup.searcher.search(fdbSetup.queryMaker.makeQuery(), cachedCollector);
            Collection topGroups = c1.getTopGroups(0);
            if (topGroups == null) {
                // No groups matched
                return;
            }
            blackhole.consume(topGroups.size());
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            fdbSetup = new FDBSearchSetup();
            fdbSetup.setSearchType(BenchmarkUtil.SearchTypeEnum.ByGroup);
            fdbSetup.startFDBNetworking();
            fdbSetup.createReader();
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception {
            fdbSetup.teardown();
        }
    }

    @State(Scope.Benchmark)
    public static class NioGroupSearchBenchmark {

        private NIOSSearchSetup nioSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchNIOSByGroup")
        @GroupThreads(1)
        public void searchNIOSByGroup(Blackhole blackhole) throws Exception {
            Sort groupSort = new Sort(SortField.FIELD_SCORE,
                    new SortField("sorteddocdate", Type.STRING));
            FirstPassGroupingCollector c1 = new FirstPassGroupingCollector(
                    new TermGroupSelector("group100"), groupSort, nioSetup.topNDocs);
            boolean cacheScores = true;
            double maxCacheRAMMB = 4.0;
            CachingCollector cachedCollector = CachingCollector.create(c1, cacheScores, maxCacheRAMMB);
            nioSetup.searcher.search(nioSetup.queryMaker.makeQuery(), cachedCollector);
            Collection topGroups = c1.getTopGroups(0);
            if (topGroups == null) {
                blackhole.consume(topGroups);
                return;
            }
            blackhole.consume(topGroups.size());
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            nioSetup = new NIOSSearchSetup();
            nioSetup.setSearchType(BenchmarkUtil.SearchTypeEnum.ByGroup);
            nioSetup.setupNIOS();
            nioSetup.createReader();
        }

        @TearDown(Level.Trial)
        public void teardown() throws Exception {
            nioSetup.teardown();
        }
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(GroupSearchBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}


