package com.sapashev;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits source file to the regions.
 * Each region is aligned according to the beginning of next tag.
 *
 */
public class Splitter {
    private final File file;
    private final int numOfRegions;

    public Splitter (File file, int numOfRegions) {
        this.file = file;
        this.numOfRegions = numOfRegions;
    }

    /**
     * Calculates the beginning and end of each region.
     * That regions will be parsed by threads concurrently.
     * @return list of Range objects (start, end) of each region.
     * @throws FileNotFoundException
     */
    public List<Range> getRegions() throws IOException{
        List<Range> ranges = new ArrayList<>();
        long fileLength = file.length();
        long regionLength = fileLength / numOfRegions;
        long start = 0;
        long end;
        try(RandomAccessFile raf = new RandomAccessFile(file, "r")){
            for(int i = 0; i < numOfRegions; i++){
                raf.seek(start + regionLength);
                end = start + regionLength + findStartOfNextTag(raf) - 1;
                end = end >= fileLength ? fileLength : end;
                ranges.add(new Range(start, end));
                start = end;
            }
        }
        return ranges;
    }

    /**
    * Finds beginning of next nearest opening tag '<'.
    * Returns offset of the opening tag from the beginning of file as current position minus two bytes. That
    * need to position pointer before opening tag.
    */
    public long findStartOfNextTag(RandomAccessFile raf) throws IOException {
        byte[] array = new byte[100];
        raf.read(array);
        String s = new String(array, "UTF-8");
        return s.indexOf('<') + 1;
    }

    public static List<Range> createRanges (int length, int numOfRanges){
        List<Range> ranges = new ArrayList<>(numOfRanges);
        int chunkLength = length / numOfRanges;
        int pointer = 0;
        for (int i = 0; i < numOfRanges; i++) {
            ranges.add(new Range(pointer, pointer + chunkLength));
            pointer = pointer + chunkLength;
        }
        return ranges;
    }

}
