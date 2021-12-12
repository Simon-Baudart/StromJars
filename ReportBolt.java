import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> compteur_final = null;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.compteur_final = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long nb_de_mots = tuple.getLongByField("count");
        this.compteur_final.put(word, nb_de_mots);
    }

    public void cleanup() {
        System.out.println("--- Nb d'occurences ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.compteur_final.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.compteur_final.get(key));
        }
        System.out.println("--------------");
    }
}