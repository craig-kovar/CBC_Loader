package com.ckdemo.cbcloader;

import com.couchbase.client.core.error.DocumentNotFoundException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LoadThread extends Thread {

    private String name;
    private String idPattern;
    private int baseIdNum = 1;

    LoadThread(String name, String idPattern) {
        this.name = name;
        this.idPattern = idPattern;
        this.baseIdNum = baseIdNum;
    }

    @Override
    public void run() {
        System.out.println("Starting Thread " + name + " at " + new Date());
        boolean run = true;

        while (run) {

            int idOffset = CBCLoader.getIdOffset();
            String getDocKey = String.format(idPattern, baseIdNum);
            String putDocKey = String.format(idPattern, idOffset);

            try {
                CBUtil.getAndLoadDoc(getDocKey, putDocKey);
                baseIdNum++;
                if (baseIdNum > 10000) {
                    baseIdNum = 1;
                }

                idOffset++;

            } catch (DocumentNotFoundException dne) {
                baseIdNum = 1;
            }
        }

        System.out.println("Completed Thread " + name + " at " + new Date());
    }

}
