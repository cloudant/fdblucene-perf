package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
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
    @State(Scope.Benchmark)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public static abstract class AbstractIndexingBenchmark {
        protected Database db;
        private Directory dir;
        private Document doc;
        private IndexWriter writer;
        private StringField idField;
        private AtomicLong counter = new AtomicLong();

        public abstract Directory getDirectory(final Path path) throws IOException;

        @Benchmark
        @Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
        @Measurement(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
        public long indexing() throws Exception {
            idField.setStringValue("doc-" + counter.incrementAndGet());
            return writer.addDocument(doc);
        }

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            final IndexWriterConfig config = indexWriterConfig();
            dir = getDirectory(generateTestPath());
            cleanDirectory();
            writer = new IndexWriter(dir, config);
            doc = new Document();
            idField = new StringField("_id", "", Store.YES);
            doc.add(idField);
            counter.set(0L);
        }

        @TearDown(Level.Iteration)
        public void teardown() throws Exception {
            writer.close();
            cleanDirectory();
        }

        private void cleanDirectory() throws IOException {
            for (final String name : dir.listAll()) {
                dir.deleteFile(name);
            }
        }

        private Path generateTestPath() {
            final FileSystem fileSystem = FileSystems.getDefault();
            final Path javaTempDir = fileSystem
                    .getPath(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));
            return javaTempDir.resolve("foo");
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
