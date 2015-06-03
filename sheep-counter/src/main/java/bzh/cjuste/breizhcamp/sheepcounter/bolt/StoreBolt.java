package bzh.cjuste.breizhcamp.sheepcounter.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import bzh.cjuste.breizhcamp.sheepcounter.entity.Color;
import bzh.cjuste.breizhcamp.sheepcounter.spout.RedisPubSubSpout;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Clement on 03/06/2015.
 */
public class StoreBolt implements IRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreBolt.class);
    private static final String REDIS_KEY = "sorted_sheeps";

    private Map<Color, AtomicInteger> sheepsByColor;
    private OutputCollector outputCollector;
    private JedisPool jedisPool;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        sheepsByColor = new HashMap<>();
        this.outputCollector = outputCollector;
        jedisPool = new JedisPool(RedisPubSubSpout.REDIS_URL);
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            sendDataRedis();
        } else if (tuple.contains("color") && tuple.contains("number")) {
            Color color = (Color) tuple.getValueByField("color");
            if (!sheepsByColor.containsKey(color)) {
                sheepsByColor.put(color, new AtomicInteger(0));
            }
            int newNumber = sheepsByColor.get(color).addAndGet(tuple.getIntegerByField("number"));
            LOGGER.debug("{} sheeps for the color {}", newNumber, color);
        }
        outputCollector.ack(tuple);
    }


    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    private void sendDataRedis() {
        Jedis jedis = jedisPool.getResource();
        for (Map.Entry<Color, AtomicInteger> sheepsEntry : sheepsByColor.entrySet()) {
            String field = sheepsEntry.getKey().toString().toLowerCase();
            int lastValue = 0;
            String lastValueString = jedis.hget(REDIS_KEY, field);
            if ( StringUtils.isNotEmpty(lastValueString)) {
                lastValue = Integer.valueOf(lastValueString);
            }
            jedis.hset(REDIS_KEY, field, String.valueOf(sheepsEntry.getValue().addAndGet(lastValue)));
        }
        sheepsByColor.clear();
        jedisPool.returnResource(jedis);
    }

    @Override
    public void cleanup() {
        jedisPool.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 30);
        return conf;
    }
}
