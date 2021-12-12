import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector flux_sortie;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        Fields champ = new Fields("word");
        ofd.declare(champ);
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.flux_sortie = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        String la_phrase_recue = tuple.getStringByField("sentence");
        String[] liste_de_mots = la_phrase_recue.split(" ");
        for(String mot : liste_de_mots){
            Values une_valeur =  new Values(mot);
            this.flux_sortie.emit(une_valeur);
        }
    }

}