package com.cloudant.fdblucene.benchmark;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import com.apple.foundationdb.FDB;
import com.cloudant.fdblucene.FDBDirectory;

public class FDBIndexingSetup extends IndexingSetup {

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
