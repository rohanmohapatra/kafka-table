package edu.sjsu.cs249.kafkaTable.cli;

import edu.sjsu.cs249.kafkaTable.servers.ServerManager;
import edu.sjsu.cs249.kafkaTable.state.KafkaState;
import edu.sjsu.cs249.kafkaTable.state.ReplicaState;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name= "replica", mixinStandardHelpOptions = true)
public class ReplicaCli implements Callable<Integer> {

    @CommandLine.Parameters(paramLabel = "kafkaHost:port")
    String kafkaServer;

    @CommandLine.Parameters(paramLabel = "replicaName")
    String replicaName;

    @CommandLine.Parameters(paramLabel = "grpcPort")
    Integer port;

    @CommandLine.Parameters(paramLabel = "snapshotTime")
    Integer snapshotTime;

    @CommandLine.Parameters(paramLabel = "topicPrefix")
    String topicPrefix;

    @Override
    public Integer call() throws Exception {
        ReplicaState.getInstance().replicaName = replicaName;
        KafkaState.getInstance().kafkaServer = kafkaServer;
        KafkaState.getInstance().topicPrefix = topicPrefix;
        KafkaState.getInstance().snapshotTime = snapshotTime;
        ServerManager serverManager = new ServerManager(port);
        serverManager.startUp();

        return 0;
    }
}
