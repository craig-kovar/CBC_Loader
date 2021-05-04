package com.ckdemo.cbcloader;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.kv.GetResult;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PerfThread extends Thread {

    private String name;
    private String idPattern;
    private int minId = 1;
    private int maxId = 1;
    private int batch = 1;
    private boolean async = false;
    private boolean print = false;

    private long successfulCnt = 0L;
    private long errorCnt = 0L;
    private long microsecondElapsed = 0L;
    private long minMicro = 1000000;
    private long maxMicro = 0;

    PerfThread(String name, String idPattern, int min, int max, int batch, boolean async, boolean print) {
        this.name = name;
        this.idPattern = idPattern;
        this.minId = min;
        this.maxId = max;
        this.batch = batch;
        this.async = async;
        this.print = print;

        if (async) {
            minMicro = -1;
            maxMicro = -1;
        }
    }

    public String getLocalName() {
        return this.name;
    }

    public long getSuccessfulCnt() {
        return this.successfulCnt;
    }

    public long getErrorCnt() {
        return this.errorCnt;
    }

    public long getMicrosecondElapsed() {
        return this.microsecondElapsed;
    }

    public long getMinMicro() {
        return this.minMicro;
    }

    public long getMaxMicro() {
        return this.maxMicro;
    }

    @Override
    public void run() {
        System.out.println(String.format("Starting thread %s at %s with batch = %s; min = %s; max = %s",
                name,new Date(), batch, minId, maxId));
        while (minId <= maxId) {
            try {

                if (!async) {
                    Instant startInstant = Instant.now();
                    GetResult result = CBUtil.getDoc(String.format(String.format(idPattern, minId)));
                    Long duration = ChronoUnit.MICROS.between(startInstant, Instant.now());

                    if (print) {
                        System.out.println(result.contentAsObject());
                    }

                    successfulCnt++;
                    microsecondElapsed += duration;

                    if (duration < minMicro) {
                        minMicro = duration;
                    }

                    if (duration > maxMicro) {
                        maxMicro = duration;
                    }

                    minId++;

                } else {

                    List<String> docsToFetch = new ArrayList<String>();
                    long startId = minId;
                    long stop = startId + batch;

                    for (long i = startId; i<stop; i++) {
                        docsToFetch.add(String.format(idPattern,startId));
                        startId++;
                        if (startId > maxId)
                            break;
                    }

                    Instant startInstant = Instant.now();
                    List<GetResult> results = CBUtil.asyncGets(docsToFetch);
                    Long duration = ChronoUnit.MICROS.between(startInstant, Instant.now());

                    if (print) {
                        for (GetResult itr : results) {
                            System.out.println(itr.contentAsObject());
                        }
                    }

                    successfulCnt += results.size();
                    errorCnt += (batch - results.size());
                    microsecondElapsed += duration;

                    minId += batch;

                }

            } catch (CouchbaseException ce) {
                errorCnt++;
            }


        }

        System.out.println("Completed Thread " + name + " at " + new Date());
    }

}
