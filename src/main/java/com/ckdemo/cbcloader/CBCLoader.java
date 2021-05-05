package com.ckdemo.cbcloader;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class CBCLoader {

    private static final String VERSION = "1.0.3";

    private static String directory;
    private static int threads = 2;
    private static CBUtil cbutil;
    private static String mode;
    private static String connstr;
    private static String username;
    private static String password;
    private static String bucketName;
    private static String idPattern;
    private static int idOffset = 1;
    private static int batch;
    private static int min;
    private static int max;
    private static boolean reactive;
    private static boolean print;

    private static AtomicInteger idOffsetCounter = new AtomicInteger(1);

    public static void main(String[] args) {
        if (args.length == 0) {
            usage();
            System.exit(0);
        }

        //Process Arguments
        for (int i =0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-m")) {
                if (++i < args.length)
                    mode = args[i].toLowerCase(Locale.ROOT);
            }

            if (args[i].equalsIgnoreCase("-c")) {
                if (++i < args.length)
                    connstr = args[i];
            }

            if (args[i].equalsIgnoreCase("-u")) {
                if (++i < args.length)
                    username = args[i];
            }

            if (args[i].equals("-p")) {
                if (++i < args.length)
                    password = args[i];
            }

            if (args[i].equals("-b")) {
                if (++i < args.length)
                    bucketName = args[i];
            }

            if (args[i].equalsIgnoreCase("-d")) {
                if (++i < args.length)
                    directory = args[i];
            }

            if (args[i].equalsIgnoreCase("-t")) {
                if (++i < args.length)
                    threads = Integer.parseInt(args[i]);
            }

            if (args[i].equalsIgnoreCase("-i")) {
                if (++i < args.length)
                    idPattern = args[i];
            }

            if (args[i].equalsIgnoreCase("-o")) {
                if (++i < args.length) {
                    idOffset = Integer.parseInt(args[i]);
                    idOffsetCounter.set(idOffset);
                }
            }

            if (args[i].equals("-B")) {
                if (++i < args.length)
                    batch = Integer.parseInt(args[i]);
            }

            if (args[i].equals("-r")) {
                if (++i < args.length)
                    min = Integer.parseInt(args[i]);
            }

            if (args[i].equals("-R")) {
                if (++i < args.length)
                    max = Integer.parseInt(args[i]);
            }

            if (args[i].equalsIgnoreCase("-a")) {
                    reactive = true;
            }

            if (args[i].equalsIgnoreCase("-P")) {
                print = true;
            }

        }

        //Verify required parameters
        if (mode == null) {
            System.out.println("Mode not specified, exiting...");
            System.exit(0);
        }

        if (connstr == null || username == null || password == null || bucketName == null) {
            System.out.println("Missing Couchbase Connection Information, exiting...");
            System.exit(0);
        }

        //Connect to Couchbase Cluster
        System.out.println("Establishing connection to CBC");
        cbutil = new CBUtil(connstr, username, password, bucketName);
        if (reactive) {
            CBUtil.allowAsync();
        }

        //Determine and execute mode
        switch (mode) {
            case "fakeit" :
                System.out.println("Running mode - fakeit");
                runFakeIt();
                break;
            case "load" :
                System.out.println("Running mode - load");
                runLoad();
                break;
            case "perf":
                System.out.println("Running mode - perf");
                runPerf();
                break;
            default:
                System.out.println("Unknown mode - " + mode);
                System.exit(0);
        }

        //Shut down our connections
        CBUtil.shutDown();

    }

    public static int getIdOffset() {
        return idOffsetCounter.getAndAdd(1);
    }

    private static void runLoad() {
        System.out.println("Running load with idOffset = " + idOffset);
        Thread[] loadThreads = new LoadThread[threads];

        for (int i = 0; i< loadThreads.length; i++) {
            loadThreads[i] = new LoadThread("T"+i, idPattern);
        }

        for (int i = 0; i< loadThreads.length; i++) {
            loadThreads[i].start();
        }

        for (int i=0; i< loadThreads.length; i++) {
            try {
                loadThreads[i].join();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void runPerf() {
        System.out.println(String.format("Running perf with batch = %s; min = %s; max = %s and reactive = %b",
                batch, min, max, reactive));
        Thread[] perfThreads = new PerfThread[threads];

        int itemPerThread = (max - min) / threads;
        int startMin = min;
        int newMax = 0;

        for (int i = 0; i< perfThreads.length; i++) {
            newMax = startMin + itemPerThread;
            if (newMax > max) {
                newMax = max;
            }
            perfThreads[i] = new PerfThread("T"+i,idPattern,startMin,newMax,batch,reactive,print);
            startMin += itemPerThread + 1;
        }

        for (int i = 0; i< perfThreads.length; i++) {
            perfThreads[i].start();
        }

        for (int i=0; i< perfThreads.length; i++) {
            try {
                perfThreads[i].join();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        printStats((PerfThread[]) perfThreads);
    }

    private static void printStats(PerfThread[] threads) {
        System.out.println("");
        System.out.println(String.format("Using Couchbase Reactive SDK = %s",reactive));
        System.out.println(String.format("|%15s|%15s|%15s|%25s|%25s|%25s|%25s|%10s|%10s|%10s|",
                "Thread Name",
                "Successful Gets",
                "Error Gets",
                "Total Elapsed Time (us)",
                "Average Elapsed Time (us)",
                "Min Elapsed Time (us)",
                "Max Elapsed Time (us)",
                "120-500ms",
                "500ms-1s",
                "1s+"));

        System.out.println("-".repeat(186));


        for (int i=0; i< threads.length; i++) {

            long avg = 0L;
            try {
                avg = threads[i].getMicrosecondElapsed()/threads[i].getSuccessfulCnt();
            } catch (ArithmeticException ex) {
                avg = 0;
            }

            System.out.println(String.format("|%15s|%15d|%15d|%25d|%25d|%25d|%25d|%10d|%10d|%10d|",
                    threads[i].getLocalName(),
                    threads[i].getSuccessfulCnt(),
                    threads[i].getErrorCnt(),
                    threads[i].getMicrosecondElapsed(),
                    avg,
                    threads[i].getMinMicro(),
                    threads[i].getMaxMicro(),
                    threads[i].getOps200ms(),
                    threads[i].getOps500ms(),
                    threads[i].getOps1s()));

            if (i%5 == 0 && i>0) {
                System.out.println("-".repeat(186));
            }
        }

        System.out.println("-".repeat(186));
        System.out.println("");
    }

    private static void runFakeIt() {
        //Parse directory and set up threads
        System.out.println("Processing Directory " + directory + " using " + threads + " Threads");

        WorkerThread[] workerThreads = new WorkerThread[threads];
        for (int i=0; i < workerThreads.length; i++) {
            workerThreads[i] = new WorkerThread("T"+i);
        }

        //File[] directories = CBCFileManager.getDirectories(directory);
        //for (File fItr : directories) {
        String subDir = directory + "/docs";
        for (int i=0; i < workerThreads.length; i++) {
            workerThreads[i].setDirectory(subDir);
        }


        File[] files = CBCFileManager.getFiles(subDir);
        int cnt = 0;
        for (File jsonItr : files) {
            workerThreads[cnt%threads].addFile(jsonItr.getName());
            cnt++;
        }


        //Run the threads
        for (int i=0; i< workerThreads.length; i++) {
            workerThreads[i].start();
        }

        //Wait for all Threads
        for (int i=0; i< workerThreads.length; i++) {
            try {
                workerThreads[i].join();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void usage() {
        System.out.println("CBC_Loader");
        System.out.println("Version: " + VERSION);
        System.out.println("");
        System.out.println("\t Required Settings");
        System.out.println("\t-m mode - The execution mode.  Valid values are fakeit, load, and perf");
        System.out.println("\t-c connection string - The Couchbase connection string");
        System.out.println("\t-u username - The username to connect to Couchbase");
        System.out.println("\t-p password - The password to connect to Couchbase");
        System.out.println("\t-t threads - The number of threads to use");
        System.out.println("");
        System.out.println("Mode = fakeit");
        System.out.println("\t-d directory - The directory where the documents will be loaded from");
        System.out.println();
        System.out.println("Mode = load");
        System.out.println("\t-i id pattern = The pattern to use for ids = I-%d");
        System.out.println("\t-o id offset = The starting value to use for ids");
        System.out.println("\t-P = print output");
        System.out.println("");
        System.out.println("Mode = perf");
        System.out.println("\t-B batch size = The number of gets to perform per batch");
        System.out.println("\t-i id pattern = The pattern to use for ids = I-%d");
        System.out.println("\t-r min id = The minimum id range to get");
        System.out.println("\t-R max id = The maximum id range to get");
        System.out.println("\t-a = Use Async Couchbase API");
        System.out.println("\t-P = print output");

    }

}
