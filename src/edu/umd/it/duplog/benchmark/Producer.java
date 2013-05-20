package edu.umd.it.duplog.benchmark;

public class Producer {
    public static void main(String[] args) {
        String prefix = "";
        if (args.length > 0) {
            prefix = args[0];
        }

        Counter counter = new Counter("Messages produced");
        counter.start();

        while (true) {
            System.out.println(prefix + " Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit " + counter.increment());
        }
    }
}
