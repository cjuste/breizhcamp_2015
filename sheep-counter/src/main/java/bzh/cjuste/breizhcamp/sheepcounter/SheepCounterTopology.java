package bzh.cjuste.breizhcamp.sheepcounter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import bzh.cjuste.breizhcamp.sheepcounter.bolt.ColorSortingBolt;
import bzh.cjuste.breizhcamp.sheepcounter.bolt.CountBolt;
import bzh.cjuste.breizhcamp.sheepcounter.spout.RedisPubSubSpout;

/**
 * Created by Clement on 03/06/2015.
 */
public class SheepCounterTopology {

    private static final String REDIS_SPOUT = "REDIS_SPOUT";
    private static final String SORT_BOLT = "SORT_BOLT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static LocalCluster localCluster;


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        boolean dev = true;
        if (args.length > 0) {
            dev = false;
        } else {
        }
        startCluster(args, dev);
        if (dev) {
            shutDown(600000);
        }
        System.exit(0);
    }

    public static void startCluster(String [] args, boolean dev) throws AlreadyAliveException,
            InvalidTopologyException {

        RedisPubSubSpout redisPubSubSpout = new RedisPubSubSpout();
        TopologyBuilder builder = new TopologyBuilder();

        // spout with 5 parallel instances
        builder.setSpout(REDIS_SPOUT, redisPubSubSpout, 1);

        // init bolt
        builder.setBolt(SORT_BOLT, new ColorSortingBolt(), 5).shuffleGrouping(REDIS_SPOUT);
        builder.setBolt(COUNT_BOLT, new CountBolt(), 5).fieldsGrouping(SORT_BOLT, new Fields("color"));

        Config conf = new Config();
        conf.setMaxSpoutPending(100);
        conf.setNumWorkers(2);
        if (!dev) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setDebug(false);
            localCluster = new LocalCluster();
            localCluster.submitTopology("sheep-counter", conf, builder.createTopology());
        }
    }

    public static void shutDown(long millis) {
        Utils.sleep(millis);
        localCluster.killTopology("sheep-counter");
        localCluster.shutdown();
    }
}
