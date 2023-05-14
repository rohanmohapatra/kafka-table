package edu.sjsu.cs249.kafkaTable.state;

import java.util.HashMap;
import java.util.Map;

public class ReplicaState{
    private static ReplicaState replicaState;

    public String replicaName;

    public Map<String, Integer> table = new HashMap<>();

    public ReplicaState() {}

    public static ReplicaState getInstance(){
        if (replicaState == null){
            replicaState = new ReplicaState();
        }
        return replicaState;
    }
}
