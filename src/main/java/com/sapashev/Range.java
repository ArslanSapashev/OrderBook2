package com.sapashev;

public class Range {
    public final long start;
    public final long end;

    public Range (long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString () {
        return start + " " + end;
    }
}
