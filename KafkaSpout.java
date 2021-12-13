public class KafkaSpout {
    // ZooKeeper connection string
    BrokerHosts hosts = new ZkHosts(zkConnString);

    //Creating SpoutConfig Object
    SpoutConfig spoutConfig = new SpoutConfig(hosts,
            topicName, "/" + topicName UUID.randomUUID().toString());

//convert the ByteBuffer to String.
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    //Assign SpoutConfig to KafkaSpout.
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
}
