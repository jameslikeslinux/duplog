package edu.umd.it.duplog.benchmark;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Consumer {
    public static void main(String[] args) throws java.io.IOException {
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        Counter counter = new Counter("Messages consumed");
        counter.start();

        while (input.readLine() != null) {
            counter.increment();
        }
    }
}
