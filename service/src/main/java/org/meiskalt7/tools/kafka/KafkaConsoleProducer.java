package org.meiskalt7.tools.kafka;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class KafkaConsoleProducer {

    static void processCommand(CommandLine commandLine) {
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
                Integer partition = commandLine.hasOption("p") ? Integer.valueOf(commandLine.getOptionValue("partition")) : null;
                ProducerRecord<String, String> message = new ProducerRecord<>(topic, partition, null, data);
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

    private static void getPaths(String dirName, List<String> paths, boolean recursively) {
        File dir = new File(dirName);
        if (dir.isDirectory()) {
            for (File item : dir.listFiles()) {
                if (item.isDirectory()) {
                    if (recursively) {
                        getPaths(item.getAbsolutePath(), paths, true);
                    }
                } else {
                    if (!item.getName().contains(".jar")) { //quickfix for current file
                        paths.add(item.getAbsolutePath());
                    }
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
        props.put("max.request.size", commandLine.hasOption("mrs") ? commandLine.getOptionValue("mrs") : 3145728);
        return new KafkaProducer<>(props);
    }
}
