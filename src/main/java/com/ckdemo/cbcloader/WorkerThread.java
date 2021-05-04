package com.ckdemo.cbcloader;

import com.couchbase.client.java.json.JsonObject;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WorkerThread extends Thread {

    private List<String> files;
    private String directory;
    private String name;
    private JSONParser parser = new JSONParser();

    WorkerThread() {
        this.files = new ArrayList<String>();
    }

    WorkerThread(String name) {
        this.files = new ArrayList<String>();
        this.name = name;
    }

    public String getDirectory() {
        return this.directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void addFile(String file) {
        if (files == null) {
            files = new ArrayList<String>();
        }

        files.add(file);
    }

    public void resetList() {
        this.files.clear();
    }

    @Override
    public void run() {
        System.out.println("Starting Thread " + name + " at " + new Date());
        for (String file : files) {
            try {
                String path = directory + "/" + file;
                Object obj = parser.parse(new FileReader(path));
                JSONObject jo = (JSONObject) obj;
                String key = file.split("\\.")[0];

                if (key != null) {
                    CBUtil.putDoc(key, jo);
                }

            } catch (IOException | ParseException fne) {
                fne.printStackTrace();
            }
        }

        System.out.println("Completed Thread " + name + " at " + new Date());
    }

}
