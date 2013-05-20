package edu.umd.it.duplog.benchmark;

public class Counter extends Thread {
    private String prefix;
    private long count = 0;

    public Counter(String prefix) {
        this.prefix = prefix;
    }

    public void run() {
        long lastCount = 0;
        long seconds = 0;

        while (true) {
            try {
                sleep(1000);
            } catch (InterruptedException ie) {
                // do nothing
            }

            long lastSecond = count - lastCount;
            long average = count / ++seconds;
            System.err.print("\r" + prefix + ": " + lastSecond + " last second / " + average + " per second average");
            lastCount = count;
        }
    }

    public long increment() {
        return ++count;
    }
}
