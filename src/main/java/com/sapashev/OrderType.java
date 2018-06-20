package com.sapashev;

public enum OrderType {
    ADD, DELETE;

    @Override
    public String toString () {
        return this.name();
    }
}
