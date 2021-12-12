import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector flux_sortie;
    private HashMap<String, Long> compteurs = null;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        Fields champ = new Fields("word", "Count");
        ofd.declare(champ);
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        this.flux_sortie = oc;
        this.compteurs = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String le_mot = tuple.getStringByField("word");
        Long valeur_courante = this.compteurs.get(le_mot);
        if(valeur_courante == null){
            valeur_courante = 0L;
        }
        valeur_courante++;
        this.compteurs.put(le_mot, valeur_courante);

        Values une_valeur =  new Values(le_mot, valeur_courante);
        this.flux_sortie.emit(une_valeur);
    }

} 