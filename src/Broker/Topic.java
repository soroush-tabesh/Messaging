package Broker;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Topic {
    private final String name;

    private final File topicFile;
    private final TopicWriter topicWriter;
    private final Map<String, TopicReader> topicReaders = Collections.synchronizedMap(new HashMap<>()); // thread-safe
    private final Object topicReaderLock = new Object();

    Topic(String name) {
        this.name = name;
        new File("./brokerTemp").mkdir();
        topicFile = new File("./brokerTemp/" + name + ".dat");
        try {
            if (topicFile.delete())
                topicFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        topicWriter = new TopicWriter(this);
    }

    public String getName() {
        return name;
    }

    public File getTopicFile() {
        return topicFile;
    }

    private void addGroup(String groupName) {
        topicReaders.put(groupName, new TopicReader(this, groupName));
    }

    /**
     * This method is used to get the first value in the topic file which is not read in the given group yet, and serve it for the appropriate consumer.
     *
     * @return the value of the first remained item.
     */
    public int get(String groupName, String consumerName) {
        synchronized (topicReaderLock) { // prevent slipped-condition
            if (!topicReaders.containsKey(groupName)) {
                addGroup(groupName);
            }
        }
        return topicReaders.get(groupName).get(consumerName);
    }

    /**
     * This method is used to put the given value at the tail of the topic file.
     *
     * @param value the value to be put at the end of the topic file
     * @return Nothing.
     */
    public void put(String producerName, int value) {
        topicWriter.put(producerName, value);
    }
}
