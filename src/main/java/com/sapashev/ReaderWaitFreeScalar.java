package com.sapashev;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class ReaderWaitFreeScalar {
    private final File file;
    private final int NUM_OF_REGIONS;
    private final char[] books;
    private final TYPE[] types;
    private final int[] prices;
    private final int[] volumes;
    private final int[] dels;
    private Map<Character, Map<Integer, LongAdder>> buys = new ConcurrentHashMap<>();
    private Map<Character, Map<Integer, LongAdder>> sells = new ConcurrentHashMap<>();

    public ReaderWaitFreeScalar (File file, int numOfRegions, int ordersCount) {
        this.file = file;
        this.NUM_OF_REGIONS = numOfRegions;
        books = new char[ordersCount];
        types = new TYPE[ordersCount];
        prices = new int[ordersCount];
        volumes = new int[ordersCount];
        dels = new int[ordersCount];
    }

    /**
     * Creates order books summarized according to their prices.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void fuseOrders () throws InterruptedException, ExecutionException {
        List<Future> futures = new ArrayList<>();
        ExecutorService service = Executors.newFixedThreadPool(NUM_OF_REGIONS);
        List<Range> fuseRanges = Splitter.createRanges(books.length, NUM_OF_REGIONS);
        fuseRanges.forEach((r) -> futures.add(service.submit(new Fuser(r))));

        for (Future f : futures) {
            f.get();
        }
        shutExecutorAfterMillisec(service, 2000);
    }

    /**
     * Transforms
     */
    public void finalMatching () {
        Map<Character, SortedMap<Integer, Integer>> bs = new HashMap<>();
        Map<Character, SortedMap<Integer, Integer>> ss = new HashMap<>();

        transformLongAdderToInteger(buys.entrySet(), bs, Comparator.reverseOrder());
        transformLongAdderToInteger(sells.entrySet(), ss, Comparator.naturalOrder());

        Set<Character> books = bs.keySet();
        for (Character c : books) {
            matcher(bs.get(c), ss.get(c));
        }
    }

    /**
    Method matches prices and sells orders.
    NB. This method expects that buy orders are sorted descending (in reverse order) and
    sell orders are sorted ascending (in natural order).
     */
    public static void matcher (SortedMap<Integer, Integer> buys, SortedMap<Integer, Integer> sells) {
        int buyPrice;
        int sellPrice;
        int buyVolume;
        int sellVolume;

        while (!buys.isEmpty() && !sells.isEmpty() && (buyPrice = buys.firstKey()) >= (sellPrice = sells.firstKey())) {
            if ((buyVolume = buys.get(buyPrice)) <= (sellVolume = sells.get(sellPrice))) {
                buys.remove(buyPrice);
                if (sellVolume - buyVolume == 0) {
                    sells.remove(sellPrice);
                } else {
                    sells.put(sellPrice, (sellVolume - buyVolume));
                }
            } else {
                buys.put(buyPrice, (buyVolume - sellVolume));
                sells.remove(sellPrice);
            }
        }
    }

    private void transformLongAdderToInteger (Set<Map.Entry<Character, Map<Integer, LongAdder>>> origin,
                                              Map<Character, SortedMap<Integer, Integer>> destination,
                                              Comparator<Integer> comparator) {

        for (Map.Entry<Character, Map<Integer, LongAdder>> entry : origin) {
            SortedMap<Integer, Integer> tempMap = new TreeMap<>(comparator);
            for (Map.Entry<Integer, LongAdder> ent : entry.getValue().entrySet()) {
                tempMap.put(ent.getKey(), ent.getValue().intValue());
            }
            destination.put(entry.getKey(), tempMap);
        }
    }

    /**
     * Shuts down executor service after specified time elapses.
     * @param service
     * @param timeout
     * @throws InterruptedException
     */
    private void shutExecutorAfterMillisec (ExecutorService service, long timeout) throws InterruptedException {
        service.shutdown();
        service.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        service.shutdownNow();
    }

    /**
     * Major method - runs all necessary processing to get OrderBook.
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void process () throws IOException, InterruptedException, ExecutionException {
        parseOrderFile();
        removeDeletedOrders();
        fuseOrders();
        finalMatching();
    }

    /**
     * Parses market orders file to fill-in arrays: types, books, prices, volumes, dels. Using Crawler object.
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void parseOrderFile () throws IOException, InterruptedException, ExecutionException {
        List<MappedByteBuffer> buffers = this.getBuffers(new RandomAccessFile(file, "r"));
        ExecutorService service = Executors.newFixedThreadPool(NUM_OF_REGIONS);
        List<Future> futures = new ArrayList<>();
        buffers.forEach((b) -> futures.add(service.submit(new Crawler(b, new Parser(b)))));
        for (Future f : futures) {
            f.get();
        }
        futures.clear();
        shutExecutorAfterMillisec(service, 2000);
    }

    /**
     * Removes deleted orders from arrays: types, books, prices, volumes, dels. Using Eraser object.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void removeDeletedOrders () throws InterruptedException, ExecutionException {
        List<Range> delRanges = Splitter.createRanges(dels.length, NUM_OF_REGIONS);
        ExecutorService service = Executors.newFixedThreadPool(NUM_OF_REGIONS);
        List<Future> futures = new ArrayList<>();
        delRanges.forEach((r) -> futures.add(service.submit(new Eraser(r))));
        for (Future f : futures) {
            f.get();
        }
        futures.clear();
        shutExecutorAfterMillisec(service, 2000);
    }

    /**
     * Splits market orders file to the regions (as MappedByteBuffers) to be processed concurrently.
     * @param raf
     * @return
     * @throws IOException
     */
    public List<MappedByteBuffer> getBuffers (RandomAccessFile raf) throws IOException {
        Splitter splitter = new Splitter(file, NUM_OF_REGIONS);
        List<Range> ranges = splitter.getRegions();
        List<MappedByteBuffer> buffers = new ArrayList<>();
        for (Range r : ranges) {
            buffers.add(raf.getChannel().map(FileChannel.MapMode.READ_ONLY, r.start, r.end - r.start - 1));
        }
        return buffers;
    }

    /**
     * Parses market order file and assigns to array fields parsed values.
     */
    private class Crawler implements Runnable {
        private final ByteBuffer buffer;
        private final Parser parser;

        public Crawler (ByteBuffer buffer, Parser parser) {
            this.buffer = buffer;
            this.parser = parser;
        }

        @Override
        public void run () {
            int stop = parser.getPositionOfLastOpeningTag();
            while (buffer.position() < stop) {
                parser.positionToNextOpenTag();
                OrderType t = parser.getOrderType();
                if (t == OrderType.ADD) {
                    char c = parser.getBook();
                    TYPE type = parser.getTYPE();
                    int price = parser.getPrice();
                    int vol = parser.getVolume();
                    int index = parser.getOrderId();
                    books[index] = c;
                    types[index] = type;
                    prices[index] = price;
                    volumes[index] = vol;
                } else if (t == OrderType.DELETE) {
                    String book = parser.getContentOfQuotes();
                    char c = book.charAt(book.length() - 1);
                    int index = Integer.parseInt(parser.getContentOfQuotes());
                    dels[index] = c;
                } else {
                    throw new RuntimeException("Could not PARSE order type ADD/DELETE");
                }
            }
        }
    }

    /**
     * Removes deleted orders from arrays, by assigning to them default (zero, null) values.
     */
    private class Eraser implements Runnable {
        private final int startInclusive;
        private final int endExclusive;

        public Eraser (Range range) {
            this.startInclusive = (int) range.start;
            this.endExclusive = (int) range.end;
        }

        @Override
        public void run () {
            for (int i = startInclusive; i < endExclusive; i++) {
                if (dels[i] != 0) {
                    books[i] = 0;
                    types[i] = null;
                    prices[i] = 0;
                    volumes[i] = 0;
                }
            }
        }
    }

    /**
     * Creates order books summarized according to their prices.
     */
    private class Fuser implements Runnable {
        private final int startInclusvive;
        private final int endExclusive;

        public Fuser (Range range) {
            this.startInclusvive = (int) range.start;
            this.endExclusive = (int) range.end;
        }

        @Override
        public void run () {
            for (int i = startInclusvive; i < endExclusive; i++) {
                if (books[i] != 0) {
                    buys.putIfAbsent(books[i], new ConcurrentHashMap<>());
                    sells.putIfAbsent(books[i], new ConcurrentHashMap<>());
                    Map<Integer, LongAdder> map;
                    if (types[i] == TYPE.BUY) {
                        map = buys.get(books[i]);
                    } else if (types[i] == TYPE.SELL) {
                        map = sells.get(books[i]);
                    } else {
                        throw new IllegalArgumentException();
                    }
                    map.putIfAbsent(prices[i], new LongAdder());
                    map.get(prices[i]).add(volumes[i]);
                }
            }
        }
    }
}
