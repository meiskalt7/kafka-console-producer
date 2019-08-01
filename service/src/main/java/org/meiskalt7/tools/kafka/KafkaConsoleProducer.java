package org.meiskalt7.tools.kafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaConsoleProducer {

    public static void main(String[] args) throws ParseException {
        Options posixOptions = createOptions();
        if (checkForHelp(args)) {
            printHelp(posixOptions);
        } else {
            CommandLine commandLine = fillParameters(args, posixOptions);
            processCommand(commandLine);
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

        Options posixOptions = new Options();
        posixOptions.addOption(kafkaTopicOption);
        posixOptions.addOption(directoryOption);
        posixOptions.addOption(recursivelyOption);
        posixOptions.addOption(encodingOption);
        posixOptions.addOption(serverOption);
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

    private static void processCommand(CommandLine commandLine) {
        List<String> filePaths;
        String dirName;
        if (commandLine.hasOption("directory")) {
            dirName = commandLine.getOptionValue("directory");
        } else {
            dirName = ".";
        }
        filePaths = new ArrayList<>();
        getPaths(dirName, filePaths, commandLine.hasOption("r"));
        String topic = commandLine.getOptionValue("topic");
        String encoding = StringUtils.isBlank(commandLine.getOptionValue("encoding"))
                ? "UTF-8"
                : commandLine.getOptionValue("encoding");
        int sendCount = 0;
        int allCount = 0;
        StopWatch sw = new StopWatch();
        sw.start();

        KafkaProducer<String, String> producer = createKafkaProducer(commandLine);
        for (String filePath : filePaths) {
            try {
                String data = FileUtils.readFileToString(new File(filePath), encoding);
                ProducerRecord<String, String> message = new ProducerRecord<>(topic, "", data);
                Future<RecordMetadata> future = producer.send(message);
                RecordMetadata recordMetadata = future.get();
                ++sendCount;
            } catch (IOException | InterruptedException | ExecutionException e) {
                System.out.println("File from " + filePath + " not loaded, cause:\n" + e.getMessage());
            }

            ++allCount;
            if (allCount % 10 == 0 || allCount == filePaths.size()) {
                System.out.println(String.format("%.2f", 100.0D * (double) allCount / (double) filePaths.size()));
            }
        }

        producer.close();
        sw.stop();
        System.out.println("Total files: " + filePaths.size());
        System.out.println("" + sendCount + " files were put in topic " + topic);
        System.out.println("It takes " + sw.getTime() / 1000L + " seconds.");
    }

    private static CommandLine fillParameters(String[] args, Options posixOptions) throws ParseException {
        CommandLineParser cmdLinePosixParser = new PosixParser();
        return cmdLinePosixParser.parse(posixOptions, args);
    }

    private static void getPaths(String dirName, List<String> paths, boolean recursively) {
        File dir = new File(dirName);
        if (dir.isDirectory()) {
            for (File item : dir.listFiles()) {
                if (item.isDirectory()) {
                    if (recursively) {
                        getPaths(item.getAbsolutePath(), paths, true);
                    }
                } else {
                    paths.add(item.getAbsolutePath());
                }
            }
        } else {
            paths.add(dir.getAbsolutePath());
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer(CommandLine commandLine) {
        Properties props = new Properties();
        String server = commandLine.getOptionValue("server");
        if (StringUtils.isBlank(server)) {
            props.put("bootstrap.servers", "localhost:9092");
        } else {
            props.put("bootstrap.servers", server);
        }

        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("max.request.size", 3145728);
        return new KafkaProducer(props);
    }
}
