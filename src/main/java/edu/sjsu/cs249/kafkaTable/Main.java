package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.simple.SimpleLogger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Command
public class Main {
    static {
        // quiet some kafka messages
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }
    Properties properties = new Properties();

    @Parameters(description = "the host:port of the kafka bootstrap server")
    void setBootStrapServers(String server) {
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Command
    int publish(@Parameters(paramLabel = "topic-name") String name) throws IOException {
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
        var br = new BufferedReader(new InputStreamReader(System.in));
        for (int i = 0;; i++) {
            var line = br.readLine();
            if (line == null) break;
            var bytes = SimpleMessage.newBuilder()
                    .setMessage(line)
                    .build().toByteArray();
            var record = new ProducerRecord<String, byte[]>(name, bytes);
            producer.send(record);
        }
        return 0;
    }

    @Command
    int consume(@Parameters(paramLabel = "topic-name") String name,
                @Parameters(paramLabel = "group-id") String id) throws InvalidProtocolBufferException {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id);
       var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(name));
        while (true) {
            var records = consumer.poll(Duration.ofSeconds(20));
            for (var record: records) {
                var message = SimpleMessage.parseFrom(record.value());
                System.out.println(message);
            }
        }
    }

    @Command
    int listTopics() throws ExecutionException, InterruptedException {
        try (var admin = Admin.create(properties)) {
            var rc = admin.listTopics();
            var listings = rc.listings().get();
            for (var l : listings) {
                System.out.println(l);
            }
        }
        return 0;
    }

    @Command
    int createTopic(@Parameters(paramLabel = "topic-name") String name) throws InterruptedException, ExecutionException {
        try (var admin = Admin.create(properties)) {
            var rc = admin.createTopics(List.of(new NewTopic(name, 1, (short) 1)));
            rc.all().get();
        }
        return 0;
    }
    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }
}