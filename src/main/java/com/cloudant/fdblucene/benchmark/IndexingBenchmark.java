package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.LineFileDocs;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.cloudant.fdblucene.FDBDirectory;


public class IndexingBenchmark {

    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @State(Scope.Benchmark)
    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.MINUTES)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public static abstract class AbstractIndexingBenchmark {
        protected Database db;
        private Directory dir;
        private LineFileDocs lineFileDocs;
        private IndexWriter writer;
        private Random random;

        public abstract Directory getDirectory(final Path path) throws IOException;

        @Benchmark
        public long addDocument() throws Exception {
            final Document doc = lineFileDocs.nextDoc();
            return writer.addDocument(doc);
        }

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            final IndexWriterConfig config = indexWriterConfig();
            dir = getDirectory(generateTestPath());
            writer = new IndexWriter(dir, config);
            random = new Random();
            lineFileDocs = new LineFileDocs(random);
        }

        @TearDown(Level.Iteration)
        public void teardown() throws Exception {
            writer.close();
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

        @Param({ "64", "128", "256", "512", "1024", "4096" })
        protected int pageSize;

        @Param({ "1", "2", "5", "10", "100" })
        protected int txnMultiplier;

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
        public Directory getDirectory(final Path path0) throws IOException {
            List<String> path1 = pathAsList(path0);
            DirectoryLayer dirLayer = DirectoryLayer.getDefault();
            dirLayer.removeIfExists(db, path1).join();
            DirectorySubspace dir = dirLayer.create(db, path1).join();
            return FDBDirectory.open(db, dir, pageSize, pageSize * txnMultiplier);
        }

        private static List<String> pathAsList(final Path path) {
            final List<String> result = new ArrayList<String>();
            for (final Path p : path) {
                result.add(p.toString());
            }
            return result;
        }

    }

    public static class NIOFSIndexingBenchmark extends AbstractIndexingBenchmark {

        @Override
        public Directory getDirectory(final Path path) throws IOException {
            final Directory result = new NIOFSDirectory(path);
            for (final String name : result.listAll()) {
                result.deleteFile(name);
            }
            return result;
        }

    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(IndexingBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}
