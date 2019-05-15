package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class ReutersIndexingBenchmark {

    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @Warmup(iterations = 3, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
    @Timeout(time = 15, timeUnit = TimeUnit.MINUTES)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Benchmark
    public long indexFDB(FDBReutersIndexingSetup setup) throws Exception {
        Document doc = setup.docMaker.makeDocument();
        return setup.writer.addDocument(doc);
    }

    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @Warmup(iterations = 3, time = 30, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.MINUTES)
    @Timeout(time = 15, timeUnit = TimeUnit.MINUTES)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Benchmark
    public long indexNIOS(NIOSReutersIndexingSetup setup) throws Exception {
        Document doc = setup.docMaker.makeDocument();
        return setup.writer.addDocument(doc);
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(ReutersIndexingBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }
}