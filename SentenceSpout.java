import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout {

    private String[] liste_de_phrases =
            {
                    "A un passant",
                    "Mon cher enfant que j'ai vu dans ma vie errante,",
                    "Mon cher enfant, que, mon Dieu, tu me recueillis,",
                    "Moi-même pauvre ainsi que toi, purs comme lys,",
                    "Mon cher enfant que j'ai vu dans ma vie errante !",
                    "Et beau comme notre âme pure et transparente,",
                    "Mon cher enfant, grande vertu de moi, la rente,",
                    "De mon effort de charité, nous, fleurs de lys !",
                    "On te dit mort... Mort ou vivant, sois ma mémoire !",
                    "Et qu'on ne hurle donc plus que c'est de la gloire",
                    "Que je m'occupe, fou qu'il fallut et qu'il faut...",
                    "Petit ! mort ou vivant, qui fis vibrer mes fibres,",
                    "Quoi qu'en aient dit et dit tels imbéciles noirs",
                    "Compagnon qui ressuscitas les saints espoirs,",
                    "Va donc, vivant ou mort, dans les espaces libres !",
                    "Paul Verlaine"
            };
    private int index = 0;
    private static int termine = 0;

    private SpoutOutputCollector flux_sortie;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

        Fields champ = new Fields("sentence");
        ofd.declare(champ);

    }

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector soc)
    {
        this.flux_sortie = soc;
    }

    public int consulter_etat() {
        int res = termine;
        return res;
    }

    @Override
    public void nextTuple() {
        if (termine==0)
        {
            String phrase = liste_de_phrases[index];
            Values valeur = new Values(phrase);
            flux_sortie.emit(valeur);
            System.out.println("Spout : emission de "+phrase);
            index++;
            if (index>=liste_de_phrases.length)
            {
                index = 0;
                termine=1;
            }
        }
        Utils.waitForMillis(1);
    }
}