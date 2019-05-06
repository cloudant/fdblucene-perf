package com.cloudant.fdblucene.benchmark;

import java.io.IOException;

import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;


public class NIOSSearchSetup extends SearchSetup {

    @Setup(Level.Trial)
    public void setupNIOS() throws Exception{
        super.setup();
    }

    @Override
    public Directory getDirectory(final Path path) throws IOException {
        return new NIOFSDirectory(path);
    }

}

