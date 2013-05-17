package edu.umd.it.duplog;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

public class Main {
    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("duplog.jar").version("Duplog 1");
        parser.addArgument("--version").action(Arguments.version());
        Subparsers subparsers = parser.addSubparsers();

        Subparser sendParser = subparsers.addParser("send").help("read log messages from stdin");

        Subparser receiveParser = subparsers.addParser("receive").help("extract deduplicated log messages").defaultHelp(true);
        receiveParser.addArgument("-o").dest("output_file").setDefault("/var/log/duplog.log").help("where to write deduplicated log messages");
        receiveParser.addArgument("-r").dest("redis_server").setDefault("localhost").help("hostname of the Redis server");
        receiveParser.addArgument("").dest("syslog_server").nargs("+").help("hostname of a syslog server running RabbitMQ");

        try {
            System.out.println(parser.parseArgs(args));
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }
}
