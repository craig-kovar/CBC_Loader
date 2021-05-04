package com.ckdemo.cbcloader;

import java.io.File;
import java.util.List;

public class CBCFileManager {

    public static File[] getDirectories(String directory) {
        File[] directories = new File(directory).listFiles(File::isDirectory);
        return directories;
    }

    public static File[] getFiles(String directory) {
        File[] directories = new File(directory).listFiles(File::isFile);
        return directories;
    }

}
