package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiQueryMaker;
import org.apache.lucene.benchmark.byTask.utils.Config;

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


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.apple.foundationdb.Database;

@State(Scope.Benchmark)
public abstract class SearchSetup {
    protected Database db;
    protected Directory dir;
    protected Document doc;
    protected DocMaker docMaker;
    protected IndexWriter writer;
    protected DirectoryReader reader;
    protected IndexSearcher searcher;
    protected int docsToIndex = 10000;
    protected EnwikiQueryMaker queryMaker;
    protected int topNDocs = 50;

    @Param({"5", "10", "50", "100"})
    protected int numberOfTerms;

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

        for (int i = 0; i < docsToIndex; i++) {
            doc = docMaker.makeDocument();
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

    private Config loadBenchConfig() throws IOException {
        Properties props = new Properties();
        InputStream in = getClass().getResourceAsStream("/content-source.properties");
        // load the docs.file of the enwiki content source or other content source
        // docs.file is used by DocMaker
        props.load(in);
        return new Config(props);
    }

    // can be overriden to use different content sources
    protected ContentSource getContentSource() {
        return new EnwikiContentSource();
    }
}
