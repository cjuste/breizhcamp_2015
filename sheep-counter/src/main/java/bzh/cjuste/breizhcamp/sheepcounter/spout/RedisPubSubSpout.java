package bzh.cjuste.breizhcamp.sheepcounter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Clement on 03/06/2015.
 */
public class RedisPubSubSpout extends JedisPubSub implements IRichSpout{

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisPubSubSpout.class);
    public static final String REDIS_URL = "localhost";
    private static final String CHANNEL = "sheeps";
    private SpoutOutputCollector spoutOutputCollector;
    private JedisPool jedisPool;
    private LinkedBlockingQueue<String> queue;
    private Thread subscribingThread;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sheeps"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        LOGGER.info("Initializing the redis connection.");
        this.spoutOutputCollector = spoutOutputCollector;
        this.queue = new LinkedBlockingQueue<>();
        jedisPool = new JedisPool(new JedisPoolConfig(), REDIS_URL);
        subscribingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Jedis jedis = jedisPool.getResource();
                jedis.subscribe(RedisPubSubSpout.this, CHANNEL);
            }
        });
    }

    @Override
    public void close() {
        jedisPool.close();
    }

    @Override
    public void activate() {
        subscribingThread.start();
    }

    @Override
    public void deactivate() {
        subscribingThread.interrupt();
    }

    @Override
    public void nextTuple() {
        String message = queue.poll();
        if (StringUtils.isEmpty(message)) {
            Utils.sleep(50);
        } else {
            LOGGER.info("Sending the message {}", message);
            this.spoutOutputCollector.emit(new Values(message));
        }
    }

    @Override
    public void ack(Object o) {
    }

    @Override
    public void fail(Object o) {
        LOGGER.warn("Failure for the tuple {}", o);
    }

    @Override
    public void onMessage(String channel, String message) {
        LOGGER.debug("Receiving the redis message {}", message);
        this.queue.offer(message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {

    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {

    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {

    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {

    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {

    }
}
