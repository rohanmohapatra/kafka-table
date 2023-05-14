package edu.sjsu.cs249.kafkaTable.servers;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.sjsu.cs249.kafkaTable.KafkaManager;
import edu.sjsu.cs249.kafkaTable.Snapshot;
import edu.sjsu.cs249.kafkaTable.SnapshotOrdering;
import edu.sjsu.cs249.kafkaTable.state.KafkaState;
import edu.sjsu.cs249.kafkaTable.state.ReplicaState;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.HashMap;

import static edu.sjsu.cs249.kafkaTable.Constants.OPERATIONS_TOPIC_NAME;
import static edu.sjsu.cs249.kafkaTable.Constants.SNAPSHOT_ORDERING_TOPIC_NAME;

/**
 * Startup the GRPC Server
 * On Startup, fetch from Snapshots Topic
 * On Startup, fetch from Snapshots Ordering Topic
 * Recreate your "Replica State"
 * Then Startup GRPC Server and bind to port, DO NOT BIND IF YOU ARE NOT READDYYY
 *
 */
public class ServerManager {
    private Integer port;

    public ServerManager(Integer port) {
        this.port = port;
    }

    public void startUp() throws IOException, InterruptedException {
        // Build GRPC server instances
        KafkaTable kafkaTableService = new KafkaTable(KafkaManager.getManager().createProducer());
        KafkaTableDebugService kafkaTableDebugService = new KafkaTableDebugService();
        Server server = ServerBuilder
                .forPort(port)
                .addService(kafkaTableService)
                .addService(kafkaTableDebugService)
                .build();

        kafkaTableDebugService.setGrpcServer(server);

        // Get Snapshots and update local states
        if (KafkaState.getInstance().snapshotQueue.isEmpty()){
            System.out.println("Snapshot queue is empty");
            var records = KafkaManager.getManager().consumeSnapshots();
            System.out.println("Got Snapshots:" + records.count());
            for (var record: records) {
                var snapshot = Snapshot.parseFrom(record.value());
//                System.out.println("Snapshot value: "+ snapshot);
                KafkaState.getInstance().snapshotQueue.addLast(snapshot);
            }
            this.buildState();
        }
        else {
            this.buildState();
        }
        System.out.println("Snapshot queue updated");

        //Publish snapshot order
        KafkaManager.getManager().shouldPublishSnapshotOrder();

        Thread thread = new Thread(() -> {
            try {
                KafkaManager.getManager().consumeOperations();
            } catch (InvalidProtocolBufferException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();

        // Start server since you are REAADYY
        server.start();
        System.out.println("Started Server at Port: " + this.port);

        server.awaitTermination();
        thread.join();
    }

    private void buildState() {
        var snapshot = KafkaState.getInstance().snapshotQueue.pollLast();
        if (snapshot != null){
            System.out.println("Snapshot used to repopulate state: " +
                    snapshot.getTableMap() + " " +
                    snapshot.getReplicaId() + " "+
                    snapshot.getClientCountersMap() + " " +
                    snapshot.getSnapshotOrderingOffset() + " " +
                    snapshot.getOperationsOffset());
            ReplicaState.getInstance().table = new HashMap<>(snapshot.getTableMap());
            KafkaState.getInstance().clientCounters = new HashMap<>(snapshot.getClientCountersMap());
            KafkaState.getInstance().operationsOffset.set(snapshot.getOperationsOffset());
            KafkaState.getInstance().snapshotOrderingOffset.set(snapshot.getSnapshotOrderingOffset());
        }

    }
}
