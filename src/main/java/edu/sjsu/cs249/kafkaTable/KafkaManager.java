package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.sjsu.cs249.kafkaTable.state.GrpcState;
import edu.sjsu.cs249.kafkaTable.state.KafkaState;
import edu.sjsu.cs249.kafkaTable.state.ReplicaState;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.javatuples.Pair;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Semaphore;

import static edu.sjsu.cs249.kafkaTable.Constants.*;

public class KafkaManager {

    private static KafkaManager manager;
    private Properties properties = new Properties();

    private KafkaProducer<String, byte[]> producer = null;
    private KafkaConsumer<String, byte[]> consumer = null;

    public KafkaManager() {
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaState.getInstance().kafkaServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ReplicaState.getInstance().replicaName);
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    }

    public static KafkaManager getManager(){
        if (manager == null){
            manager = new KafkaManager();
        }
        return manager;
    }

    public KafkaProducer<String, byte[]> createProducer(){
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaState.getInstance().kafkaServer);
        return new KafkaProducer<>(properties, new StringSerializer(), new ByteArraySerializer());
    }

    public KafkaProducer<String, byte[]> getProducer(){
        if (producer == null) {
            this.producer = createProducer();
        }
        return this.producer;
    }

    public KafkaConsumer<String, byte[]> getConsumer(){
        if (consumer == null) {
            this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        }
        return this.consumer;
    }

    public void createSnapshotOrder(){
        // Produce Snapshot ordering during startup
        var producer = KafkaManager.getManager().getProducer();
        String topicName = KafkaState.getInstance().topicPrefix + SNAPSHOT_ORDERING_TOPIC_NAME;
        var bytes = SnapshotOrdering.newBuilder()
                .setReplicaId(ReplicaState.getInstance().replicaName)
                .build()
                .toByteArray();
        var snapshotOrderRecord = new ProducerRecord<String, byte[]>(topicName, bytes);
        producer.send(snapshotOrderRecord);
        System.out.println("Snapshot order created");
    }

    public ConsumerRecords<String, byte[]> consumeSnapshots() throws InterruptedException {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ReplicaState.getInstance().replicaName+"-snapshot");
        var consumer = getConsumer();
        String topicName = KafkaState.getInstance().topicPrefix + SNAPSHOT_TOPIC_NAME;
        var semaphore = new Semaphore(3, true);
        consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                consumer.seekToBeginning(collection);
                semaphore.release();
                System.out.println("Snapshot semaphore released");
            }
        });

        consumer.poll(0);
        System.out.println("Snapshot first poll");
        semaphore.acquire();
        System.out.println("Snapshot semaphore acquire");
        var records = consumer.poll(Duration.ofSeconds(20));
        consumer.commitSync();
        return records;
    }

    public ConsumerRecords<String, byte[]> consumeSnapshotOrdering(long offset) throws InterruptedException {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ReplicaState.getInstance().replicaName+"-snapshotorder");
        var consumer = getConsumer();
        String topicName = KafkaState.getInstance().topicPrefix + SNAPSHOT_ORDERING_TOPIC_NAME;
        var semaphore = new Semaphore(3, true);
        consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                consumer.seekToBeginning(collection);
                semaphore.release();
                System.out.println("Snapshot ordering semaphore released");
            }
        });

        consumer.poll(0);
        System.out.println("Snapshot ordering first poll");
        semaphore.acquire();
        System.out.println("Snapshot ordering semaphore acquire");
        consumer.seek(consumer.assignment().stream().findFirst().get(), offset);
        System.out.println("Snapshot ordering seek done");

        var records = consumer.poll(Duration.ofSeconds(20));
        consumer.commitSync();
        return records;
    }

    public void consumeOperations() throws InvalidProtocolBufferException, InterruptedException {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ReplicaState.getInstance().replicaName+"-operation");
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        String topicName = KafkaState.getInstance().topicPrefix + OPERATIONS_TOPIC_NAME;
        System.out.println("Listening on " + topicName);
        var semaphore = new Semaphore(3, true);
        System.out.println("Operations Semaphore created");
        consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                consumer.seekToBeginning(collection);
                semaphore.release();
                System.out.println("Operations Semaphore released");
            }
        });

        System.out.println("In consume operations " + consumer.poll(0).count());
        semaphore.acquire();
        // Seek to last offset
        consumer.seek(consumer.assignment().stream().findFirst().get(), KafkaState.getInstance().operationsOffset.get() + 1);
        System.out.println("Operations Semaphore acquired");


        while (true) {
            var records = consumer.poll(Duration.ofSeconds(20));
            for (var record: records) {
                var item = PublishedItem.parseFrom(record.value());
                if (item.hasGet()) {
                    // No - op
                    synchronized (GrpcState.getInstance().pendingGets) {
                        var getRequest = item.getGet();
                        System.out.println("Get Request from kafka:" + getRequest);
                        if (GrpcState.getInstance().pendingGets.containsKey(getRequest.getXid())) {
                            StreamObserver<GetResponse> responseObserver = GrpcState.getInstance().pendingGets.get(getRequest.getXid());
                            System.out.println(ReplicaState.getInstance().table);
                            try {
                                responseObserver.onNext(GetResponse.newBuilder()
                                        .setValue(ReplicaState.getInstance().table.getOrDefault(getRequest.getKey(), 0))
                                        .build());
                                responseObserver.onCompleted();
                            }
                            catch (Exception e){
                                // no-op
                                System.out.println(e.getMessage());
                            }
                            GrpcState.getInstance().pendingIncs.remove(getRequest.getXid());
                            KafkaState.getInstance().clientCounters.put(getRequest.getXid().getClientid(), getRequest.getXid().getCounter());
                        }

                    }
                } else if (item.hasInc()) {
                    // Check if the clientXid and counter is greater than anything we have from user
                    // Then add to table with increment
                    StreamObserver<IncResponse> responseObserver = null;
                    var incRequest = item.getInc();
                    System.out.println("Inc Request from kafka:" + incRequest);
                    if (incRequest.getXid().getCounter() >
                            KafkaState.getInstance().clientCounters.getOrDefault(incRequest.getXid().getClientid(), 0)
                    ) {
                        Integer value = ReplicaState.getInstance().table.getOrDefault(incRequest.getKey(), 0);
                        if (value + incRequest.getIncValue() >= 0) {
                            ReplicaState.getInstance().table.put(incRequest.getKey(), value + incRequest.getIncValue());
                            System.out.println("Put into table");
                        }

                        KafkaState.getInstance().clientCounters.put(incRequest.getXid().getClientid(), incRequest.getXid().getCounter());
                    }
                    synchronized (GrpcState.getInstance().pendingIncs) {
                        if (GrpcState.getInstance().pendingIncs.containsKey(incRequest.getXid())) {
                            System.out.println("Client Xid: for Pending Incs: " + incRequest.getXid());
                            responseObserver = GrpcState.getInstance().pendingIncs.get(incRequest.getXid());
                            GrpcState.getInstance().pendingIncs.remove(incRequest.getXid());
                        }
                    }

                    System.out.println("Trying to send back inc request: " + (responseObserver != null));
                    if (responseObserver != null) {
                        System.out.println("Sent back inc request");
                        // TODO: Bug :  Stream is already completed, no further calls are allowed
                        try {
                            responseObserver.onNext(IncResponse.newBuilder().build());
                            responseObserver.onCompleted();
                        }
                        catch (Exception e){
                            // no-op
                            System.out.println(e.getMessage());
                        }
                    }
                }
                consumer.commitSync();
                // Could be deprecated
                if (false) {
                    KafkaState.getInstance().operationsOffset.set(record.offset());
                    System.out.println("Operations offset is set");
                    if (record.offset() % KafkaState.getInstance().snapshotTime == 0
                            && record.offset() != 0) {
                        System.out.println("Trying to publish a snapshot");
                        //Check if you can publish snapshot
                        var snapshotOrdering = this.consumeSnapshotOrdering(KafkaState.getInstance().snapshotOrderingOffset.get() + 1);
                        System.out.println("Snapshot ordering retrieved");
                        for (var snapshotOrderingRecord : snapshotOrdering) {
                            System.out.println("Snapshot ordering Record: " + SnapshotOrdering.parseFrom(snapshotOrderingRecord.value()) + " " + snapshotOrderingRecord.offset());
                            if (SnapshotOrdering.parseFrom(snapshotOrderingRecord.value()).getReplicaId().equals(ReplicaState.getInstance().replicaName)) {
                                var snapshotTopicName = KafkaState.getInstance().topicPrefix + SNAPSHOT_TOPIC_NAME;
                                var producer = this.getProducer();
                                var snapshotRecordBytes = KafkaState.getInstance().createSnapshot().toByteArray();
                                var snapshotRecord = new ProducerRecord<String, byte[]>(snapshotTopicName, snapshotRecordBytes);
                                producer.send(snapshotRecord);
                                System.out.println("Snapshot published");
                                this.createSnapshotOrder();
                            }
                            KafkaState.getInstance().snapshotOrderingOffset.set(snapshotOrderingRecord.offset());
                            break;
                        }
                    }
                }

                if (true) {
                    KafkaState.getInstance().operationsOffset.set(record.offset());
                    System.out.println("Operations offset is set");
                    if ((record.offset()) % KafkaState.getInstance().snapshotTime == 0) {
                        System.out.println("Trying to publish a snapshot");
                        //Check if you can publish snapshot
                        var snapshotOrderingRecord = this.getSnapshotOrder();
                        System.out.println("Snapshot ordering retrieved");
                        System.out.println("Snapshot ordering Record: " + snapshotOrderingRecord);
                        KafkaState.getInstance().snapshotOrderingOffset.set(snapshotOrderingRecord.getValue1());
                        if (snapshotOrderingRecord.getValue0().getReplicaId().equals(ReplicaState.getInstance().replicaName)){
                            var snapshotTopicName = KafkaState.getInstance().topicPrefix + SNAPSHOT_TOPIC_NAME;
                            var producer = this.getProducer();
                            var snapshotRecordBytes = KafkaState.getInstance().createSnapshot().toByteArray();
                            var snapshotRecord = new ProducerRecord<String, byte[]>(snapshotTopicName, snapshotRecordBytes);
                            producer.send(snapshotRecord);
                            System.out.println("Snapshot published");
                            this.createSnapshotOrder();
                        }
                    }
                }
            }
        }
    }

    public void shouldPublishSnapshotOrder() throws InterruptedException, InvalidProtocolBufferException {
        var shouldPublish = true;
        var records = consumeSnapshotOrdering(KafkaState.getInstance().snapshotOrderingOffset.get() + 1);
        for (var record: records) {
            var snapshotOrder = SnapshotOrdering.parseFrom(record.value());
            if (snapshotOrder.getReplicaId().equals(ReplicaState.getInstance().replicaName)){
                System.out.println("Found myself in snapshotOrder at offet:" + record.offset());
                shouldPublish = false;
                break;
            }
        }

        if (shouldPublish){
            createSnapshotOrder();
        }
    }

    private Pair<SnapshotOrdering, Long> getSnapshotOrder() throws InterruptedException, InvalidProtocolBufferException {
        if (KafkaState.getInstance().snapshotOrderQueue.isEmpty()){
            System.out.println("Snapshot Order queue is empty");
            var records = consumeSnapshotOrdering(KafkaState.getInstance().snapshotOrderingOffset.get() + 1);
            System.out.println("Got Snapshot Orders:" + records.count());
            for (var record: records) {
                var snapshotOrder = SnapshotOrdering.parseFrom(record.value());
                KafkaState.getInstance().snapshotOrderQueue.addLast(new Pair<>(snapshotOrder, record.offset()));
            }
        }
        return KafkaState.getInstance().snapshotOrderQueue.pollFirst();
    }
}
