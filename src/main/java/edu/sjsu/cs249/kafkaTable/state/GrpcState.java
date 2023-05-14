package edu.sjsu.cs249.kafkaTable.state;

import edu.sjsu.cs249.kafkaTable.*;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;

public class GrpcState {

    private static GrpcState grpcState;
    public Map<ClientXid, StreamObserver<GetResponse>> pendingGets = new HashMap<>();
    public Map<ClientXid, StreamObserver<IncResponse>> pendingIncs = new HashMap<>();

    public GrpcState() {}

    public static GrpcState getInstance(){
        if (grpcState == null){
            grpcState = new GrpcState();
        }
        return grpcState;
    }
}
