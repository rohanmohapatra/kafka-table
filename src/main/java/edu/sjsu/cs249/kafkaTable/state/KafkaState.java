package edu.sjsu.cs249.kafkaTable.state;

import edu.sjsu.cs249.kafkaTable.Snapshot;
import edu.sjsu.cs249.kafkaTable.SnapshotOrdering;
import org.javatuples.Pair;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaState {
    private static KafkaState kafkaState;

    public String topicPrefix;

    public String kafkaServer;

    public Integer snapshotTime;

    public Deque<Snapshot> snapshotQueue = new ArrayDeque<>();
    public Deque<Pair<SnapshotOrdering, Long>> snapshotOrderQueue = new ArrayDeque<>();

    public Map<String, Integer> clientCounters = new HashMap<>();

    public AtomicLong operationsOffset = new AtomicLong(0);

    public AtomicLong snapshotOrderingOffset = new AtomicLong(0);

    public KafkaState() {}

    public static KafkaState getInstance(){
        if (kafkaState == null){
            kafkaState = new KafkaState();
        }
        return kafkaState;
    }

    public Snapshot createSnapshot() {
        var snapshot = Snapshot.newBuilder()
                .setReplicaId(ReplicaState.getInstance().replicaName)
                .putAllTable(ReplicaState.getInstance().table)
                .setSnapshotOrderingOffset(snapshotOrderingOffset.get())
                .setOperationsOffset(operationsOffset.get())
                .putAllClientCounters(clientCounters)
                .build();
        System.out.println("Created a snapshot: "+ snapshot);
        return snapshot;
    }
}
