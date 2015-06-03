package bzh.cjuste.breizhcamp.sheepcounter;

import bzh.cjuste.breizhcamp.sheepcounter.entity.Color;
import bzh.cjuste.breizhcamp.sheepcounter.spout.RedisPubSubSpout;
import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by Clement on 03/06/2015.
 */
public class SendSheeps {

    public static final int MAX_VALUE=100;

    public static void main(String[] args) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        JedisPool jedisPool = new JedisPool(config, RedisPubSubSpout.REDIS_URL);
        Jedis jedis = jedisPool.getResource();
        jedis.expire("sorted_sheeps", -1);
        jedisPool.returnResource(jedis);
        ExecutorService executorPool = Executors.newFixedThreadPool(10);

        List<Future<Sheeps>> futureSheeps = new ArrayList<>(10);
        for (int i=0; i<10; i++) {
            futureSheeps.add(executorPool.submit(new SendSheepRunnable(jedisPool)));
        }

        Map<Color, Integer> result = new HashMap<>();
        for (Future<Sheeps> sheepsFuture : futureSheeps) {
            try {
                Sheeps sheeps = sheepsFuture.get();
                if (result.containsKey(sheeps.getColor())) {
                    result.put(sheeps.getColor(), sheeps.getNumber() + result.get(sheeps.getColor()));
                } else {
                    result.put(sheeps.getColor(), sheeps.getNumber());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        executorPool.shutdownNow();

        System.out.println("finished");
        System.out.println(result);
    }

    public static class SendSheepRunnable implements Callable<Sheeps> {

        private JedisPool jedisPool;
        private Sheeps sheeps;

        public SendSheepRunnable(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
            sheeps = new Sheeps();
            int colorIndex = (int) Math.floor(Math.random() * Color.values().length);
            sheeps.setColor(Color.values()[colorIndex]);
        }

        @Override
        public Sheeps call() throws Exception {
            Jedis jedis = jedisPool.getResource();
            JSONObject sheepsJson = new JSONObject();
            sheepsJson.put("color", sheeps.getColor().toString());
            int retries = 0;
            while (retries <=10000) {
                int numberSheeps = (int) (Math.floor(Math.random()*MAX_VALUE+1));
                sheeps.setNumber(sheeps.getNumber() + numberSheeps);
                sheepsJson.put("number", numberSheeps);
                jedis.publish("sheeps", sheepsJson.toString());
                retries++;
            }
            jedisPool.returnResource(jedis);
            return sheeps;
        }
    }
}
