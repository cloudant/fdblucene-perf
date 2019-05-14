package com.cloudant.fdblucene.benchmark;

import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

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
        @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchFDB")
        @GroupThreads(1)
        public void searchFDB() throws Exception {
            int randomSearchPosition = fdbSetup.random.nextInt(fdbSetup.searchTermList.size());
            String term = fdbSetup.searchTermList.get(randomSearchPosition);
            // we don't actually care about the number of hits
            fdbSetup.searcher.search(new TermQuery(new Term("body", term)), fdbSetup.topNDocs);
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            fdbSetup = new FDBSearchSetup();
            fdbSetup.startFDBNetworking();
            fdbSetup.createReader();
        }
    }

    @State(Scope.Benchmark)
    public static class NioSearchBenchmark {
        protected NIOSSearchSetup nioSetup;

        @BenchmarkMode(Mode.Throughput)
        @Fork(1)
        @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.MINUTES)
        @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
        @OutputTimeUnit(TimeUnit.SECONDS)
        @Benchmark
        @Group("searchNIOS")
        @GroupThreads(1)
        public void searchNIOS() throws Exception {
            int randomSearchPosition = nioSetup.random.nextInt(nioSetup.searchTermList.size());
            String term = nioSetup.searchTermList.get(randomSearchPosition);
            // we don't actually care about the number of hits
            nioSetup.searcher.search(new TermQuery(new Term("body", term)), nioSetup.topNDocs);
        }

        @Setup(Level.Trial)
        public void setup() throws Exception {
            nioSetup = new NIOSSearchSetup();
            nioSetup.setupNIOS();
            nioSetup.createReader();
        }
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(BasicSearchBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}
