package com.cloudant.fdblucene.benchmark;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.fdblucene.DocIDAllocator;

@BenchmarkMode(Mode.Throughput)
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DocIdAllocatorBenchmark {

    private Database db;
    private DocIDAllocator allocator;

    @Param({ "1", "10", "100", "1000" })
    private int allocationCount;

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        FDB.selectAPIVersion(600);
        this.db = FDB.instance().open();
        final Subspace index = new Subspace(new byte[] { 4, 5, 6 });
        db.run(txn -> {
            txn.clear(index.range());
            return null;
        });
        this.allocator = new DocIDAllocator(index);
    }

    @Benchmark
    public int[] allocate() {
        return allocator.allocate(db, allocationCount);
    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(DocIdAllocatorBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}
