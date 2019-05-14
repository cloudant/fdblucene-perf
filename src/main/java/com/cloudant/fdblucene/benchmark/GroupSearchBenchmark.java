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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Level;
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
        @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.MINUTES)
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
    }

    @State(Scope.Benchmark)
    public static class NioGroupSearchBenchmark {

        private NIOSSearchSetup nioSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.MINUTES)
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
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(GroupSearchBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}


