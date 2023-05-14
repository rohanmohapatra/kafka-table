package edu.sjsu.cs249.kafkaTable.servers;

import edu.sjsu.cs249.kafkaTable.*;
import edu.sjsu.cs249.kafkaTable.state.GrpcState;
import edu.sjsu.cs249.kafkaTable.state.KafkaState;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static edu.sjsu.cs249.kafkaTable.Constants.OPERATIONS_TOPIC_NAME;

public class KafkaTable extends KafkaTableGrpc.KafkaTableImplBase {
    private KafkaProducer<String, byte[]> producer;

    public KafkaTable(KafkaProducer<String, byte[]> producer) {
        this.producer = producer;
    }

    @Override
    public void inc(IncRequest request, StreamObserver<IncResponse> responseObserver) {
        String topicName = KafkaState.getInstance().topicPrefix + OPERATIONS_TOPIC_NAME;
        synchronized (GrpcState.getInstance().pendingIncs){
            GrpcState.getInstance().pendingIncs.put(request.getXid(), responseObserver);
        }
        var bytes = PublishedItem.newBuilder()
                .setInc(request)
                .build()
                .toByteArray();
        var record = new ProducerRecord<String, byte[]>(topicName, bytes);
        producer.send(record);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String topicName = KafkaState.getInstance().topicPrefix + OPERATIONS_TOPIC_NAME;
        synchronized (GrpcState.getInstance().pendingGets){
            GrpcState.getInstance().pendingGets.put(request.getXid(), responseObserver);
        }
        var bytes = PublishedItem.newBuilder()
                .setGet(request)
                .build()
                .toByteArray();
        var record = new ProducerRecord<String, byte[]>(topicName, bytes);
        producer.send(record);
    }
}
