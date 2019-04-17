package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.document.Document;
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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.apple.foundationdb.Database;

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
    public int docsToIndex = 100;
    public List<String> searchTermList = new ArrayList<String>();
    public int topNDocs = 50;
    public int maxSearchTerms = 10;

    public abstract Directory getDirectory(final Path path) throws IOException;

    public void setup() throws Exception {
        final IndexWriterConfig config = indexWriterConfig();
        dir = getDirectory(generateTestPath());
        cleanDirectory();
        writer = new IndexWriter(dir, config);
        random = new Random();
        for (int i = 0; i < docsToIndex; i++) {
            docs = new LineFileDocs(random, LuceneTestCase.DEFAULT_LINE_DOCS_FILE);
            doc = docs.nextDoc();
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

            idField = new StringField("_id", "", Store.YES);
            idField.setStringValue("doc-" + counter.incrementAndGet());
            doc.add(idField);
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
        return fileSystem.getPath(dir);
    }

    protected IndexWriterConfig indexWriterConfig() {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(false);
        config.setCodec(new Lucene80Codec());
        return config;
    }
}
