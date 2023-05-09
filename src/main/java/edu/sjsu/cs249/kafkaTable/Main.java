package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannelBuilder;
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
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Command
public class Main {
    static {
        // quiet some kafka messages
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    }

    @Command
    int publish(@Parameters(paramLabel = "kafkaHost:port") String server,
                @Parameters(paramLabel = "topic-name") String name) throws IOException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
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
    int consume(@Parameters(paramLabel = "kafkaHost:port") String server,
                @Parameters(paramLabel = "topic-name") String name,
                @Parameters(paramLabel = "group-id") String id) throws InvalidProtocolBufferException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, id);
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(name));
        while (true) {
            var records = consumer.poll(Duration.ofSeconds(1));
            for (var record: records) {
                System.out.println(record.headers());
                System.out.println(record.timestamp());
                System.out.println(record.timestampType());
                System.out.println(record.offset());
                var message = SimpleMessage.parseFrom(record.value());
                System.out.println(message);
            }
        }
    }

    @Command
    int listTopics(@Parameters(paramLabel = "kafkaHost:port") String server) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
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
    int createTopic(@Parameters(paramLabel = "kafkaHost:port") String server,
                    @Parameters(paramLabel = "topic-name") String name) throws InterruptedException, ExecutionException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            var rc = admin.createTopics(List.of(new NewTopic(name, 1, (short) 1)));
            rc.all().get();
        }
        return 0;
    }

    @Command(description = "delete the operations, snapshotOrder, and snapshot topics for a given prefix")
    int deleteTableTopics(@Parameters(paramLabel = "kafkaHost:port") String server,
                          @Parameters(paramLabel = "prefix") String prefix) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            List<String> topics = List.of(
                    prefix + "operations",
                    prefix + "snapshot",
                    prefix + "snapshotOrdering"
            );
            admin.deleteTopics(topics);
            System.out.println("deleted topics: " + Arrays.toString(topics.toArray()));
        }
        return 0;
    }
    @Command(description = "create the operations, snapshotOrder, and snapshot topics for a given prefix")
    int createTableTopics(@Parameters(paramLabel = "kafkaHost:port") String server,
                          @Parameters(paramLabel = "prefix") String prefix) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        try (var admin = Admin.create(properties)) {
            var rc = admin.createTopics(List.of(
                    new NewTopic(prefix + "operations", 1, (short) 1),
                    new NewTopic(prefix + "snapshot", 1, (short) 1),
                    new NewTopic(prefix + "snapshotOrdering", 1, (short) 1)
                    ));
            rc.all().get();
        }
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
        var result = producer.send(new ProducerRecord<>(prefix + "snapshot", Snapshot.newBuilder()
                .setReplicaId("initializer")
                .setOperationsOffset(0)
                .setSnapshotOrderingOffset(0)
                .putAllTable(Map.of())
                .putAllClientCounters(Map.of())
                .build().toByteArray()));
        result.get();
        return 0;

    }
    @Command
    int get(@Parameters(paramLabel = "key") String key,
            @Parameters(paramLabel = "clientId") String id,
            @Parameters(paramLabel = "grpcHost:port") String server) {
        var clientXid = ClientXid.newBuilder().setClientid(id).setCounter((int)(System.currentTimeMillis()/1000)).build();
        var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
        var rsp = stub.get(GetRequest.newBuilder().setKey(key).setXid(clientXid).build());
        System.out.println(rsp.getValue());
        return 0;
    }

        @Command
    int inc(@Parameters(paramLabel = "key") String key,
            @Parameters(paramLabel = "amount") int amount,
            @Parameters(paramLabel = "clientId") String id,
            @Option(names = "--repeat") boolean repeat,
            @Option(names = "--concurrent") boolean concurrent,
            @Parameters(paramLabel = "grpcHost:port", arity = "1..*") String[] servers) {
        int count = repeat ? 2 : 1;
        var clientXid = ClientXid.newBuilder().setClientid(id).setCounter((int)(System.currentTimeMillis()/1000)).build();
        System.out.println(clientXid);
        for (int i = 0; i < count; i++) {
            var s = Arrays.stream(servers);
            if (concurrent) s = s.parallel();
            var result = s.map(server -> {
                var stub = KafkaTableGrpc.newBlockingStub(ManagedChannelBuilder.forTarget(server).usePlaintext().build());
                try {
                    stub.inc(IncRequest.newBuilder().setKey(key).setIncValue(amount).setXid(clientXid).build());
                    return server + ": success";
                } catch (Exception e) {
                    return server + ": " + e.getMessage();
                }
            }).collect(Collectors.joining(", "));
            System.out.println(result);
        }
        return 0;
    }
    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }
}