package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.io.InputStream;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiQueryMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.apple.foundationdb.Database;
import org.openjdk.jmh.runner.FailureAssistException;

@State(Scope.Benchmark)
public abstract class SearchSetup {
    public Database db;
    public Directory dir;
    public Document doc;
    public IndexWriter writer;
    public DirectoryReader reader;
    public IndexSearcher searcher;
    public StringField idField;
    public AtomicLong counter = new AtomicLong();
    public Random random;
    public LineFileDocs docs;
    public int docsToIndex = 100000;
    public List<String> searchTermList = new ArrayList<String>();
    public int topNDocs = 50;
    public int maxSearchTerms = 1000;
    protected EnwikiQueryMaker queryMaker;
    public BenchmarkUtil.SearchTypeEnum searchType = BenchmarkUtil.SearchTypeEnum.Default;
    protected DocMaker docMaker;
    protected ContentSource source;

    public abstract Directory getDirectory(final Path path) throws IOException;

    public void setup() throws Exception {
        final IndexWriterConfig config = indexWriterConfig();
        dir = getDirectory(generateTestPath());
        cleanDirectory();
        writer = new IndexWriter(dir, config);

        Config benchConfig = loadBenchConfig();
        ContentSource source = getContentSource();
        source.setConfig(benchConfig);
        docMaker = new DocMaker();
        docMaker.setConfig(benchConfig, source);
        docMaker.resetInputs();

		// for generating queries in our benchmark
        queryMaker = new EnwikiQueryMaker();
        queryMaker.setConfig(benchConfig);
        random = new Random();

        final BytesRef[] group100 = BenchmarkUtil.randomStrings(100, random);
        final BytesRef[] group10K = BenchmarkUtil.randomStrings(10000, random);
        final BytesRef[] group100K = BenchmarkUtil.randomStrings(100000, random);
        final BytesRef[] group1M = BenchmarkUtil.randomStrings(1000000, random);

        Field group100Field = new SortedDocValuesField("group100", new BytesRef());
        Field group100KField = new SortedDocValuesField("group10K", new BytesRef());
        Field group10KField = new SortedDocValuesField("group100K", new BytesRef());
        Field group1MField = new SortedDocValuesField("group1M", new BytesRef());
        Field groupBlockField = new SortedDocValuesField("groupblock", new BytesRef());
        Field groupEndField = new StringField("groupend", "x", Field.Store.NO);

        for (int i = 0; i < docsToIndex; i++) {
            doc = docMaker.makeDocument();
            // Look through the body's terms, grab a String term, store it
            // so that it can be randomly chosen for search later on
            String[] body = doc.getValues("body");
            String[] terms = null;
            if(body.length > 0) {
                terms = body[0].split("\\s+");
            }
            if(terms !=null && searchTermList.size() < maxSearchTerms) {
                int randomTermPosition = random.nextInt(terms.length);
                searchTermList.add(terms[randomTermPosition]);
            }

            if (searchType == BenchmarkUtil.SearchTypeEnum.Default) {
                idField = new StringField("_id", "", Store.YES);
                idField.setStringValue("doc-" + counter.incrementAndGet());
                doc.add(idField);
            } else if (searchType == BenchmarkUtil.SearchTypeEnum.BySort) {
                String[] docdate = doc.getValues("docdate");
                doc.add(new SortedDocValuesField ("sorteddocdate",
                        new BytesRef(docdate[0])));
            } else if (searchType == BenchmarkUtil.SearchTypeEnum.ByGroup) {
                group100Field.setBytesValue(group100[i%100]);
                group10KField.setBytesValue(group10K[i%10000]);
                group100KField.setBytesValue(group100K[i%100000]);
                group1MField.setBytesValue(group1M[i%1000000]);

                String[] docdate = doc.getValues("docdate");
                doc.add(new SortedDocValuesField ("sorteddocdate",
                        new BytesRef(docdate[0])));
                doc.add(group100Field);
                doc.add(group10KField);
                doc.add(group100KField);
                doc.add(group1MField);
                doc.add(groupBlockField);
                doc.add(groupEndField);
            } else {
                throw new IllegalArgumentException();
            }
            writer.addDocument(doc);
        }
        writer.commit();
        System.out.println("Commited Indexing");
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

    protected void cleanDirectory() throws IOException {
        for (final String name : dir.listAll()) {
            dir.deleteFile(name);
        }
    }

    protected Path generateTestPath() {
        final String dir = System.getProperty("dir");
        if (dir == null){
            throw new Error("System property 'dir' not set.");
        }
        final FileSystem fileSystem = FileSystems.getDefault();
        return fileSystem.getPath(dir + "/index");
    }

    protected IndexWriterConfig indexWriterConfig() {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setCodec(new Lucene80Codec());
        return config;
    }

    // can be overriden to use different content sources
    protected ContentSource getContentSource() {
        return new EnwikiContentSource();
    }

    protected void setSearchType(BenchmarkUtil.SearchTypeEnum searchType) {
        this.searchType = searchType;
    }

    protected BenchmarkUtil.SearchTypeEnum getSearchType() {
        return searchType;
    }

    private Config loadBenchConfig() throws IOException {
        Properties props = new Properties();
        InputStream in = getClass().getResourceAsStream("/content-source.properties");
        // load the docs.file of the enwiki content source or other content source
        // docs.file is used by DocMaker
        props.load(in);
        return new Config(props);
    }
}
