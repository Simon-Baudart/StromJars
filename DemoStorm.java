import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class DemoStorm {

    private static final String SPOUT_ID = "Spout-generateur-de-phrases";
    private static final String BOLT_DECOUPE_ID = "bolt-decoupage-en-mots";
    private static final String BOLT_COMPTEUR_ID = "bolt-compteur-de-mots";
    private static final String BOLT_AFFICHAGE_ID = "bolt-affichage";
    private static final String TOPOLOGY_NOM = "Topology-compter-les-mots";


    public static void main(String[] args) throws Exception {

        SentenceSpout spout_genere_phrases = new SentenceSpout();
        SplitSentenceBolt bolt_decoupe = new SplitSentenceBolt();
        WordCountBolt bolt_compte = new WordCountBolt();
        ReportBolt bolt_affiche = new ReportBolt();
        TopologyBuilder topologie = new TopologyBuilder();

        topologie.setSpout(SPOUT_ID, spout_genere_phrases);
        topologie.setBolt(BOLT_DECOUPE_ID, bolt_decoupe).shuffleGrouping(SPOUT_ID);
        topologie.setBolt(BOLT_COMPTEUR_ID,
                bolt_compte).fieldsGrouping(BOLT_DECOUPE_ID, new Fields("word"));
        topologie.setBolt(BOLT_AFFICHAGE_ID,
                bolt_affiche).globalGrouping(BOLT_COMPTEUR_ID);


        Config configuration = new Config();

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], configuration, topologie.createTopology());
        }
        else
        {

            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology(TOPOLOGY_NOM, configuration, topologie.createTopology());

            int i=1;
            while (spout_genere_phrases.consulter_etat()==0)
            {
                Utils.waitForSeconds(1);
            }
            Utils.waitForSeconds(20);

            cluster.killTopology(TOPOLOGY_NOM);
            cluster.shutdown();

        }
    }

}