package com.sapashev;


import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Parses market order file
 */
public class Parser {
    private final ByteBuffer buffer;
    private final long limit;

    public Parser (ByteBuffer buffer) {
        this.buffer = buffer;
        this.limit = buffer.limit();
    }

    /**
     * Places pointer to the next opening tag position.
     */
    public void positionToNextOpenTag () {
        for (int i = buffer.position(); i < this.limit; i++) {
            char c1 = (char) buffer.get(i);
            char c2 = (char) buffer.get(i + 1);
            if (c1 == '<' && (c2 == 'A' || c2 == 'D')){
                buffer.position(++i);
                break;
            }
        }
    }

    /**
     * Returns position of last opening tag.
     * @return
     */
    public int getPositionOfLastOpeningTag () {
        int end = buffer.limit();
        int position = -1;
        char z;
        for (int i = (end - 1); i > 0; i--) {
            char c = (char) buffer.get(i);
            if (c == '<') {
                if ((z = (char) buffer.get(i + 1)) == 'A' || z == 'D') {
                    position = i;
                    break;
                }
            }
        }
        return position;
    }

    /**
     * Returns order type - to add, or to delete order from order book.
     *
     * @return order type.
     */
    public OrderType getOrderType () {
        OrderType result = null;
        char c = (char)buffer.get();
        if (c == 'A') {
            result = OrderType.ADD;
        } else if (c == 'D'){
            result = OrderType.DELETE;
        }
        return result;
    }

    public char getBook () {
        return (char) buffer.get(buffer.position() + 19);
    }

    public TYPE getTYPE () {
        buffer.position(buffer.position() + 19);
        char c = (char) buffer.get(buffer.position() + 14);
        return c == 'S' ? TYPE.SELL : TYPE.BUY;
    }

    public int getPrice () {
        buffer.position(buffer.position() + 24);
        return (int) (Float.parseFloat(getContentOfQuotes()) * 100);
    }

    public int getVolume () {
        buffer.position(buffer.position() + 7);
        return Integer.parseInt(getContentOfQuotes());
    }

    public int getOrderId () {
        return Integer.parseInt(getContentOfQuotes());
    }

    public String getContentOfQuotes () {
        char c;
        int counter = 0;
        char[] temp = new char[10];
        while ((char) buffer.get() != '"') ;
        while ((c = (char) buffer.get()) != '"' && counter < temp.length) {
            temp[counter++] = c;
        }
        return new String(Arrays.copyOf(temp, counter));
    }
}
