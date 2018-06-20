package com.sapashev;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Starter {
    public static void main (String[] args) throws IOException, ExecutionException, InterruptedException{
        File marketOrdersFile = new File(args[0]);
        int ordersCount = Integer.parseInt(args[1]);
        int threadsCount = Runtime.getRuntime().availableProcessors();
        ReaderWaitFreeScalar reader = new ReaderWaitFreeScalar(marketOrdersFile, threadsCount, ordersCount);
        reader.process();
    }
}
