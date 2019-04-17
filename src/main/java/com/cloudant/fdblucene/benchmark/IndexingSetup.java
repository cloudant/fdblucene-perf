package com.cloudant.fdblucene.benchmark;

import com.apple.foundationdb.Database;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource;
import org.apache.lucene.benchmark.byTask.utils.Config;

import org.apache.lucene.codecs.lucene80.Lucene80Codec;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public abstract class IndexingSetup {
    protected Database db;
    private Directory dir;
    protected IndexWriter writer;
    protected DocMaker docMaker;
    protected ContentSource source;

    public abstract Directory getDirectory(final Path path) throws IOException;

    @Setup(Level.Iteration)
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