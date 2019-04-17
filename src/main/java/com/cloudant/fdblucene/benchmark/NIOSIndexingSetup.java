package com.cloudant.fdblucene.benchmark;

import java.io.IOException;

import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

public class NIOSIndexingSetup extends IndexingSetup {

    @Override
    public Directory getDirectory(final Path path) throws IOException {
        return new NIOFSDirectory(path);
    }

}