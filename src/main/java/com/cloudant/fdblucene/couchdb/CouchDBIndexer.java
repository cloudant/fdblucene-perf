package com.cloudant.fdblucene.couchdb;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;

import com.google.gson.stream.JsonReader;

/**
 * Index a CouchDB/Cloudant database
 *
 */
public final class CouchDBIndexer {

    public void index(
            final String username,
            final String password,
            final String hostname,
            final String database,
            final IndexWriter writer) throws Exception {
        final String url = String.format("https://%s/%s/_changes?include_docs=true", hostname, database);
        final HttpGet httpGet = new HttpGet(url);

        try (final CloseableHttpClient httpClient = createHttpClient(username, password, hostname)) {
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed.");
                }
                final Reader inputStreamReader = new InputStreamReader(response.getEntity().getContent(), "UTF-8");
                try (final JsonReader jsonReader = new JsonReader(inputStreamReader)) {
                    indexResults(jsonReader, writer);
                }
            }
        }
        writer.commit();
    }

    private void indexResults(final JsonReader jsonReader, final IndexWriter writer) throws IOException {
        jsonReader.beginObject();
        while (jsonReader.hasNext()) {
            if ("results".equals(jsonReader.nextName())) {
                jsonReader.beginArray();
                while (jsonReader.hasNext()) {
                    jsonReader.beginObject();
                    while (jsonReader.hasNext()) {
                        if ("doc".equals(jsonReader.nextName())) {
                            final Document doc = new Document();
                            jsonReader.beginObject();
                            while (jsonReader.hasNext()) {
                                buildDocument("", jsonReader, doc);
                            }
                            jsonReader.endObject();
                            System.out.println(doc);
                            writer.addDocument(doc);
                        } else {
                            jsonReader.skipValue();
                        }
                    }
                    jsonReader.endObject();
                }
                jsonReader.endArray();
            } else {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
    }

    private void buildDocument(final String prefix, final JsonReader in, final Document out) throws IOException {
        switch (in.peek()) {
        case STRING:
            out.add(new StringField(prefix, in.nextString(), Store.YES));
            break;
        case BOOLEAN:
            out.add(new StringField(prefix, Boolean.toString(in.nextBoolean()), Store.YES));
            break;
        case NUMBER:
            out.add(new DoublePoint(prefix, in.nextDouble()));
            break;
        case BEGIN_OBJECT:
            in.beginObject();
            while (in.hasNext()) {
                buildDocument(prefix, in, out);
            }
            in.endObject();
            break;
        case BEGIN_ARRAY:
            in.beginArray();
            while (in.hasNext()) {
                buildDocument(prefix, in, out);
            }
            in.endArray();
            break;
        case NAME:
            final String name = in.nextName();
            buildDocument(String.format("%s.%s",  prefix, name), in, out);
            break;
        default:
            in.skipValue();
            break;
        }
    }

    private CloseableHttpClient createHttpClient(final String username, final String password, final String hostname) {
        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(hostname, 443), new UsernamePasswordCredentials(username, password));
        return HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
    }

}
