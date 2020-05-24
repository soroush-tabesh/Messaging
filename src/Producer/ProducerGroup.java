package Producer;

import Broker.MessageBroker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ProducerGroup extends Thread {
    private final List<Producer> producers;
    private final File producerGroupDirectory;
    private final MessageBroker messageBroker;
    private final String topicName;

    public ProducerGroup(MessageBroker messageBroker, File producerGroupDirectory, String topicName) {
        this.messageBroker = messageBroker;
        this.producerGroupDirectory = producerGroupDirectory;
        this.topicName = topicName;
        producers = new ArrayList<>();
    }

    private void initialize() {
        for (File file : producerGroupDirectory.listFiles()) {
            producers.add(new Producer(messageBroker, topicName, file.getName(), file));
        }
    }

    public void run() {
        initialize();

        for (Producer producer : producers) {
            producer.start();
        }
    }
}
