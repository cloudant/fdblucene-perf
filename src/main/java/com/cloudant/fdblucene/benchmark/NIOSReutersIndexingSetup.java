package com.cloudant.fdblucene.benchmark;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.ReutersContentSource;

public class NIOSReutersIndexingSetup extends NIOSIndexingSetup {

    @Override
    protected ContentSource getContentSource() {
        return new ReutersContentSource();
    }

}