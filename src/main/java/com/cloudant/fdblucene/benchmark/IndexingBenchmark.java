package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.cloudant.fdblucene.FDBDirectory;


public class IndexingBenchmark {

    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @State(Scope.Benchmark)
    @Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.MINUTES)
    @Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public static abstract class AbstractIndexingBenchmark {
        protected Database db;
        private Directory dir;
        private Document doc;
        private IndexableField[] fields;
        private IndexWriter writer;
        private Random random;
        private AtomicLong counter = new AtomicLong();

        @Param({ "1", "2", "5", "10", "50", "100", "200" })
        public int fieldCount;

        public abstract Directory getDirectory(final Path path) throws IOException;

        @Benchmark
        public long indexing() throws Exception {
            ((StringField) fields[0]).setStringValue("doc-" + counter.getAndIncrement());
            for (int i = 1; i < fieldCount; i++) {
                switch (i % 3) {
                case 0:
                    // Stick with original text.
                    break;
                case 1:
                    ((IntPoint) fields[i]).setIntValue(random.nextInt());
                    break;
                case 2:
                    ((DoublePoint) fields[i]).setDoubleValue(random.nextDouble());
                    break;
                }
            }
            return writer.addDocument(doc);
        }

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            final IndexWriterConfig config = indexWriterConfig();
            dir = getDirectory(generateTestPath());
            cleanDirectory();
            writer = new IndexWriter(dir, config);
            random = new Random();
            doc = new Document();
            fields = new IndexableField[fieldCount];
            fields[0] = new StringField("_id", "", Store.YES);
            for (int i = 1; i < fieldCount; i++) {
                final String fieldName = "f-" + i;
                switch (i % 3) {
                case 0:
                    fields[i] = new TextField(fieldName, "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                            Store.NO);
                    break;
                case 1:
                    fields[i] = new IntPoint(fieldName, random.nextInt());
                    break;
                case 2:
                    fields[i] = new DoublePoint(fieldName, random.nextDouble());
                    break;
                }
            }
            for (int i = 0; i < fieldCount; i++) {
                doc.add(fields[i]);
            }
            counter.set(0L);
        }

        @TearDown(Level.Iteration)
        public void teardown() throws Exception {
            writer.close();
            cleanDirectory();
        }

        private void cleanDirectory() throws IOException {
            if (dir instanceof FDBDirectory) {
                ((FDBDirectory) dir).delete();
            } else {
                for (final String name : dir.listAll()) {
                    dir.deleteFile(name);
                }
            }
        }

        private Path generateTestPath() {
            final String dir = System.getProperty("dir");
            if (dir == null){
                throw new Error("System property 'dir' not set.");
            }
            final FileSystem fileSystem = FileSystems.getDefault();
            return fileSystem.getPath(dir);
        }

        private IndexWriterConfig indexWriterConfig() {
            final IndexWriterConfig config = new IndexWriterConfig();
            config.setUseCompoundFile(false);
            config.setCodec(new Lucene80Codec());
            return config;
        }
    }

    public static class FDBIndexingBenchmark extends AbstractIndexingBenchmark {

        @Setup(Level.Trial)
        public void startFDBNetworking() {
            FDB.selectAPIVersion(600);
            db = FDB.instance().open();
        }

        @TearDown(Level.Trial)
        public void closeFDB() {
            db.close();
        }

        @Override
        public Directory getDirectory(final Path path) throws IOException {
            return FDBDirectory.open(db, path);
        }

    }

    public static class NIOFSIndexingBenchmark extends AbstractIndexingBenchmark {

        @Override
        public Directory getDirectory(final Path path) throws IOException {
            return new NIOFSDirectory(path);
        }

    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(IndexingBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}
