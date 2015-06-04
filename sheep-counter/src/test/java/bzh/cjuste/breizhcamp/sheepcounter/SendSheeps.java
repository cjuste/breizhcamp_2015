package bzh.cjuste.breizhcamp.sheepcounter;

import bzh.cjuste.breizhcamp.sheepcounter.entity.Color;
import bzh.cjuste.breizhcamp.sheepcounter.spout.RedisPubSubSpout;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Classe de test pour envoyer les données sur Redis.
 * Envoie les données sur le redis spécifié en RedisPubSubSpout.REDIS_URL
 */
public class SendSheeps {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendSheeps.class);
    private static final int MAX_VALUE=10;
    private static final int RETRIES=10000;

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
            } catch (InterruptedException|ExecutionException e) {
                LOGGER.warn("Unable to get all the futures", e);
            }
        }
        executorPool.shutdown();

        LOGGER.info("Finished sending the sheeps :\n", result);
    }

    private static class SendSheepRunnable implements Callable<Sheeps> {

        private JedisPool jedisPool;
        private Sheeps sheeps;

        public SendSheepRunnable(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
            sheeps = new Sheeps();
            int colorIndex = (int) Math.floor(Math.random() * Color.values().length);
            sheeps.setColor(Color.values()[colorIndex]);
        }

        /**
         * Envoie 1-MAX_VALUE * RETRIES moutons de la même couleur (choisie aléatoirement) sur Redis.
         * @return le nombre et la couleur des moutons envoyés
         * @throws Exception
         */
        @Override
        public Sheeps call() throws Exception {
            Jedis jedis = jedisPool.getResource();
            JSONObject sheepsJson = new JSONObject();
            sheepsJson.put("color", sheeps.getColor().toString());
            int retries = 0;
            while (retries <=RETRIES) {
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
