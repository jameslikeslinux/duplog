package edu.umd.duplog;

public class Main {
    private static void usage() {
        System.err.println("Usage: duplog.jar <send|receive>");
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            usage();
        }

        if (args[0].equals("send")) {
            Send.send();
        } else if (args[0].equals("receive")) {
            Recv.recv();
        } else {
            usage();
        }
    }
}
