package com.cloudant.fdblucene.benchmark;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.cloudant.fdblucene.FDBIndexWriter;

@BenchmarkMode(Mode.All)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 0)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FDBIndexWriterBenchmark {

    private static final Subspace index = new Subspace(new byte[] { 1, 2, 3 });

    private static Database db;
    private Document[] docs;

    private final AtomicLong counter = new AtomicLong();

    private static final int docsPerTxn = 100;

    private FDBIndexWriter writer;

    @Setup(Level.Trial)
    public void startFDBNetworking() {
        FDB.selectAPIVersion(600);
        db = FDB.instance().open();
    }

    @Setup(Level.Iteration)
    public void setup() {
        docs = new Document[docsPerTxn];
        for (int i = 0; i < docsPerTxn; i++) {
            docs[i] = doc("doc-" + i);
        }
        writer = new FDBIndexWriter(db, index, new StandardAnalyzer());
        teardown();
    }

    @TearDown(Level.Iteration)
    public void teardown() {
        db.run(txn -> {
            counter.set(0);
            txn.clear(index.range());
            return null;
        });
    }

    @Benchmark
    @OperationsPerInvocation(docsPerTxn)
    public void addDocument() throws Exception {
        try (Transaction txn = db.createTransaction()) {
            txn.options().setTransactionLoggingEnable("jmh_addDocument()");
            for (int i = 0; i < docsPerTxn; i++) {
                int id = (int) counter.incrementAndGet();
                writer.addDocument(txn, id, docs[i]);
            }
            txn.commit().join();
        }
    }

    private Document doc(final String id) {
        final Document result = new Document();
        result.add(new StringField("_id", id, Store.YES));
        result.add(new TextField("body", "abc def ghi", Store.NO));
        result.add(new StoredField("float", 123.456f));
        result.add(new StoredField("double", 123.456));
        return result;
    }

}