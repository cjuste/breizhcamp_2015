package bzh.cjuste.breizhcamp.sheepcounter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bzh.cjuste.breizhcamp.sheepcounter.entity.Color;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Bolt pour trier les moutons reçus par couleur.
 * Renvoie la couleur dans "color" et le nombre de moutons dans "number"
 */
public class ColorSortingBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColorSortingBolt.class);
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.contains("sheeps")) {
                String sheepsString = tuple.getStringByField("sheeps");
                LOGGER.debug("Receiving the message {}", sheepsString);
                JSONObject sheeps = JSONObject.fromObject(sheepsString);
                if (sheeps.containsKey("color") && sheeps.containsKey("number")) {
                    String color = sheeps.getString("color");
                    int number = sheeps.getInt("number");

                    Color colorEnum;
                    try {
                        colorEnum = Color.valueOf(color.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        colorEnum = Color.UNKNOWN;
                    }
                    outputCollector.emit(new Values(colorEnum, number));
                }
            }
            outputCollector.ack(tuple);
        } catch (Exception e) {
            LOGGER.error("Unable to sort the sheeps by color", e);
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("color", "number"));
    }
}
