import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaStormTest {
    public static void main(String[] args) throws Exception {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("192.168.100.7" + 9092, "TestDB.dbo.Inventory").build()), 1);
        tp.setBolt("bolt", new PrintBolt()).shuffleGrouping("kafka_spout");
        Config stormConfig = new Config();
        stormConfig.setDebug(true);
        stormConfig.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Test", stormConfig, tp.createTopology());
    }
}
