package org.meiskalt7.tools.kafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import java.io.PrintWriter;

/**
 * Main console logic like creation options, printing help and calling Kafka producer logic
 */
public class ConsoleApplication {

    public static void main(String[] args) throws ParseException {
        Options posixOptions = createOptions();
        if (checkForHelp(args)) {
            printHelp(posixOptions);
        } else {
            CommandLine commandLine = fillParameters(args, posixOptions);
            KafkaConsoleProducer.processCommand(commandLine);
        }
    }

    private static Options createOptions() {
        Option kafkaTopicOption = Option.builder("t")
                .longOpt("topic")
                .argName("topic")
                .desc("Kafka topic")
                .hasArg()
                .required()
                .build();

        Option partitionOption = Option.builder("p")
                .longOpt("partition")
                .argName("partition")
                .desc("Kafka partition")
                .hasArg()
                .build();

        Option directoryOption = Option.builder("d")
                .longOpt("directory")
                .argName("directory")
                .desc("Directory with xml files (current directory by default)")
                .hasArg()
                .build();

        Option recursivelyOption = Option.builder("r")
                .longOpt("recursively")
                .argName("recursively")
                .desc("Look for files recursively or not")
                .build();

        Option encodingOption = Option.builder("e")
                .longOpt("encoding")
                .argName("encoding")
                .desc("Encoding of files")
                .hasArg()
                .build();

        Option serverOption = Option.builder("s")
                .longOpt("server")
                .argName("server")
                .desc("Server path, localhost:9092 by default")
                .hasArg()
                .build();

        Option sizeOption = Option.builder("mrs")
                .longOpt("max.request.size")
                .argName("max.request.size")
                .desc("Maximum size of message")
                .hasArg()
                .build();

        Options posixOptions = new Options();
        posixOptions.addOption(kafkaTopicOption);
        posixOptions.addOption(partitionOption);
        posixOptions.addOption(directoryOption);
        posixOptions.addOption(recursivelyOption);
        posixOptions.addOption(encodingOption);
        posixOptions.addOption(serverOption);
        posixOptions.addOption(sizeOption);
        return posixOptions;
    }

    private static boolean checkForHelp(String[] args) throws ParseException {
        Option helpOption = Option.builder("h")
                .longOpt("help")
                .argName("help")
                .desc("Help")
                .build();
        Options posixOptions = new Options();
        posixOptions.addOption(helpOption);
        DefaultParser defaultParser = new DefaultParser();
        CommandLine commandLine = defaultParser.parse(posixOptions, args, true);
        return commandLine.hasOption("h");
    }

    private static void printHelp(final Options options) {
        final String commandLineSyntax = "java kafka-console-producer-*.jar";
        final PrintWriter writer = new PrintWriter(System.out);
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(
                writer, 80, commandLineSyntax,
                "Options", options, 2,
                3, "-- HELP --", true);
        writer.flush();
    }

    private static CommandLine fillParameters(String[] args, Options posixOptions) throws ParseException {
        CommandLineParser cmdLinePosixParser = new PosixParser();
        return cmdLinePosixParser.parse(posixOptions, args);
    }
}
