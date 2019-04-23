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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.document.TextField;
import java.util.List;
import java.util.ArrayList;


public class SearchBenchmark {

    @BenchmarkMode(Mode.Throughput)
    @Fork(1)
    @State(Scope.Benchmark)
    @Threads(1)
    @Warmup(iterations = 3, time = 30, timeUnit = TimeUnit.SECONDS)
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
        private List<String> searchTermList = new ArrayList<String>();
        private int topNDocs = 1;
        private int maxSearchTerms = 10000;

        public abstract Directory getDirectory(final Path path) throws IOException;

        @Benchmark
        @Group("searchrand")
        @GroupThreads(1)
        public void searchRandom() throws Exception {
            int randomSearchPosition = random.nextInt(2);
            searcher.search(new TermQuery(new Term("foorand"+randomSearchPosition, "champions")), topNDocs);
        }

        @Benchmark
        @Group("searchfixed")
        @GroupThreads(1)
        public void searchFixed() throws Exception {
            // Even though the search parameter is random, the field names
            // were known before-hand, mainly foo0 and foo1
            int randomSearchPosition = random.nextInt(2);
            searcher.search(new TermQuery(new Term("foo"+randomSearchPosition, "champions")), topNDocs);
        }

        public void setup() throws Exception {
            final IndexWriterConfig config = indexWriterConfig();
            dir = getDirectory(generateTestPath());
            cleanDirectory();
            writer = new IndexWriter(dir, config);
            random = new Random();
            for (int i = 0; i < docsToIndex; i++) {
                doc = new Document();
                int fooval  = i%2;

                // dynamic fields (only 2)
                doc.add(new TextField("foorand"+fooval, "we are the champions of the world-"+i, Store.YES));

                // fields are known beforehand
                doc.add(new TextField("foo"+0, "we are the champions of the world-"+i, Store.YES));
                doc.add(new TextField("foo"+1, "we are the champions of the world-"+i, Store.YES));

                idField = new StringField("_id", "", Store.YES);
                idField.setStringValue("doc-" + counter.incrementAndGet());
                doc.add(idField);
                writer.addDocument(doc);
            }
            writer.commit();
            writer.close();
        }

        @Setup(Level.Iteration)
        public void createReader() throws Exception {
            reader = DirectoryReader.open(dir);
            searcher = new IndexSearcher(reader);
        }

        @TearDown(Level.Iteration)
        public void teardown() throws Exception {
            reader.close();
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
        public void startFDBNetworking() throws Exception {
            FDB.selectAPIVersion(600);
            db = FDB.instance().open();
            super.setup();
        }

        @TearDown(Level.Trial)
        public void closeFDB() throws Exception {
            super.cleanDirectory();
            db.close();
        }

        @Override
        public Directory getDirectory(final Path path) throws IOException {
            return FDBDirectory.open(db, path);
        }
    }

    public static class NIOFSIndexingBenchmark extends AbstractSearchBenchmark {

        @Setup(Level.Trial)
        public void setupNIOS() throws Exception{
            super.setup();
        }

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
