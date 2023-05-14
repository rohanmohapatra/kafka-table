package edu.sjsu.cs249.kafkaTable.servers;

import edu.sjsu.cs249.kafkaTable.*;
import edu.sjsu.cs249.kafkaTable.state.KafkaState;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;

public class KafkaTableDebugService extends KafkaTableDebugGrpc.KafkaTableDebugImplBase {

    private Server grpcServer;

    public void setGrpcServer(Server grpcServer){
        this.grpcServer = grpcServer;
    }

    @Override
    public void debug(KafkaTableDebugRequest request, StreamObserver<KafkaTableDebugResponse> responseObserver) {
        synchronized (this.grpcServer){
            var snapshot = KafkaState.getInstance().createSnapshot();
            responseObserver.onNext(KafkaTableDebugResponse.newBuilder()
                            .setSnapshot(snapshot)
                            .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        synchronized (this.grpcServer){
            responseObserver.onNext(ExitResponse.newBuilder().build());
            responseObserver.onCompleted();

            this.grpcServer.shutdown();
            System.exit(0);
            System.out.println("Graceful Shutdown");
        }
    }
}
