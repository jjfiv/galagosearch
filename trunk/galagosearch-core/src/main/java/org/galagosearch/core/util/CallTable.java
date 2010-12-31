package org.galagosearch.core.util;

import gnu.trove.TObjectLongHashMap;
import gnu.trove.TObjectLongProcedure;
import java.io.PrintStream;

/**
 * Small class that can be used to track number of calls to methods
 *
 * @author irmarc
 */
public class CallTable {
    private static TObjectLongHashMap counts = new TObjectLongHashMap();
    private static boolean on = true;
    private CallTable() {}

    public static void increment(String counterName, int inc) {
        if (on) {
            counts.adjustOrPutValue(counterName, inc, inc);
        }
    }

    public static void increment(String counterName) {
        if (on) {
            counts.adjustOrPutValue(counterName, 1, 1);
        }
    }

    public static void reset() {
        counts.clear();
    }

    private static class Printer implements TObjectLongProcedure {
        PrintStream out;

        public Printer(PrintStream out) {
            this.out = out;
        }

        public boolean execute(Object a, long b) {
            out.printf("CALL_TABLE:\t%s\t%d\n", ((String) a), b);
            return true;
        }
    }

    public static void print(PrintStream out) {
        Printer p = new Printer(out);
        counts.forEachEntry(p);
    }

    public static void turnOn() { on = true; }
    public static void turnOff() { on = false; }
    public static boolean getStatus() { return on; }
}
