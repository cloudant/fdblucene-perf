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
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.cloudant.fdblucene.FDBDirectory;


public class SearchBenchmark {

    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @State(Scope.Benchmark)
    @Threads(1)
    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.MINUTES)
    @Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
    @OutputTimeUnit(TimeUnit.SECONDS)

    public static abstract class AbstractSearchBenchmark {
        protected Database db;
        private Directory dir;
        private Document doc;
        private IndexWriter writer;
        private DirectoryReader reader;
        private IndexSearcher searcher;
        private StringField idField;
        private AtomicLong counter = new AtomicLong();
        private Random random;
        private LineFileDocs docs;
        private int docsToIndex = 100;

        @Param({"true", "false"})
        private boolean bigDocs;

        public abstract Directory getDirectory(final Path path) throws IOException;

        @Benchmark
        @Group("search")
        @GroupThreads(1)
        public void search() throws Exception {
            TopDocs hits = searcher.search(new TermQuery(new Term("foo", "bar")), 10);
        }


        @Setup(Level.Iteration)
        // We should move this to Level.Trial, but so far
        // Level.Trial is only allowed once?
        public void setup() throws Exception {
            final IndexWriterConfig config = indexWriterConfig();
            dir = getDirectory(generateTestPath());
            cleanDirectory();
            writer = new IndexWriter(dir, config);
            random = new Random();
            for (int i = 0; i < docsToIndex; i++) {
                docs = new LineFileDocs(random, LuceneTestCase.DEFAULT_LINE_DOCS_FILE);
               if (bigDocs) {
                    doc = docs.nextDoc();
                } else {
                    doc = new Document();
                }
                idField = new StringField("_id", "", Store.YES);
                idField.setStringValue("doc-" + counter.incrementAndGet());
                doc.add(idField);
                writer.addDocument(doc);
            }
            writer.commit();
            writer.close();

            reader = DirectoryReader.open(dir);
            searcher = new IndexSearcher(reader);
        }

        @TearDown(Level.Iteration)
        public void teardown() throws Exception {
            reader.close();
            cleanDirectory();
        }

        private void cleanDirectory() throws IOException {
            for (final String name : dir.listAll()) {
                dir.deleteFile(name);
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

    public static class FDBSearchBenchmark extends AbstractSearchBenchmark {

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

    public static class NIOFSIndexingBenchmark extends AbstractSearchBenchmark {

        @Override
        public Directory getDirectory(final Path path) throws IOException {
            return new NIOFSDirectory(path);
        }

    }

    public static void main(final String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder().include(SearchBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }

}
