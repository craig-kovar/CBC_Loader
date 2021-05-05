package com.ckdemo.cbcloader;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import org.json.simple.JSONObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CBUtil {

    private static String connstr;
    private static String user;
    private static String password;
    private static Cluster cluster;
    private static Bucket bucket;
    private static Collection collection;
    private static ClusterEnvironment env;
    private static ReactiveCluster reactiveCluster;
    private static ReactiveBucket reactiveBucket;
    private static ReactiveCollection reactiveCollection;
    private static boolean reactive = false;

    CBUtil(String connstr, String user, String password, String bucketName) {
        this.connstr = connstr;
        this.user = user;
        this.password = password;

        env = ClusterEnvironment.builder()
                .securityConfig(SecurityConfig.enableTls(true)
                        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
                .ioConfig(IoConfig.enableDnsSrv(true))
                .build();

        // Initialize the Connection
        cluster = Cluster.connect(connstr,
                ClusterOptions.clusterOptions(user, password).environment(env));
        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.parse("PT10S"));
        collection = bucket.defaultCollection();
    }

    public static void allowAsync() {
        reactive = true;
        reactiveCluster = cluster.reactive();
        reactiveBucket = bucket.reactive();
        reactiveCollection = collection.reactive();
    }

    public static MutationResult putDoc(String key, JSONObject jo) {
        try {
            return collection.upsert(key, jo);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public static MutationResult putDoc(String key, JsonObject jo) {
        try {
            return collection.upsert(key, jo);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    public static GetResult getDoc(String key) throws CouchbaseException {
        try {
            return collection.get(key);
        } catch (CouchbaseException ex) {
            throw ex;
        }
    }

    public static List<GetResult> asyncGets(List<String> docsToFetch){
        List<GetResult> successfulResults = Collections.synchronizedList(new ArrayList<>());

        try {
            Flux.fromIterable(docsToFetch).flatMap(key -> reactiveCollection.get(key).onErrorResume(e -> {
                return Mono.empty();
            })).doOnNext(successfulResults::add).blockLast();
        } catch (RequestCanceledException re) {

        }

        return successfulResults;
    }

    public static void shutDown() {
        cluster.disconnect();
        env.shutdown();
    }

    public static void getAndLoadDoc(String getDocKey, String putDocKey) throws DocumentNotFoundException{
        try {
            GetResult getResult = collection.get(getDocKey);
            JsonObject jo = getResult.contentAsObject();
            putDoc(putDocKey, jo);
        } catch (DocumentNotFoundException dne) {
            throw dne;
        } catch (CouchbaseException ce) {
            System.out.println("Suppressing all other exceptions");
        }
    }



}
