package Broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MessageBroker {
    private final Map<String, Topic> topics = Collections.synchronizedMap(new HashMap<>());
    private final Object topicsLock = new Object();
    private final Logger logger = Logger.getInstance();

    private void addTopic(String name) {
        topics.put(name, new Topic(name));
    }

    public void put(String topic, String producerName, int value) {
        logger.log(String.format("PUT request on topic '%s' from producer '%s' with value '%d'",
                producerName, topic, value));
        synchronized (topicsLock) { //prevent slipped-condition
            if (!topics.containsKey(topic)) {
                addTopic(topic);
            }
        }
        topics.get(topic).put(producerName, value);
    }

    public int get(String topic, String groupName, String consumerName) throws NoSuchTopicException {
        logger.log(String.format("GET request on topic '%s' from consumer '%s' of group '%s'",
                consumerName, groupName, topic));
        if (!topics.containsKey(topic))
            throw new NoSuchTopicException(topic);
        return topics.get(topic).get(groupName, consumerName);
    }
}
