package Consumer;

import Broker.MessageBroker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup extends Thread {
    private final List<Consumer> consumers;
    private final MessageBroker messageBroker;
    private final String topicName;
    private final String groupName;
    private final int numberOfConsumers;

    private final File consumerGroupFile;
    private PrintWriter printWriter;

    public ConsumerGroup(MessageBroker messageBroker, String topicName, String groupName
            , File consumerGroupFile, int numberOfConsumers) {
        this.messageBroker = messageBroker;
        this.consumerGroupFile = consumerGroupFile;
        this.topicName = topicName;
        this.groupName = groupName;
        this.numberOfConsumers = numberOfConsumers;
        consumers = new ArrayList<>();
    }

    private void initialize() throws FileNotFoundException {
        for (int i = 0; i < numberOfConsumers; i++) {
            String consumerName = groupName + "_" + i;
            consumers.add(new Consumer(this, consumerName));
        }

        printWriter = new PrintWriter(consumerGroupFile);
    }

    public void run() {
        try {
            initialize();

            for (Consumer consumer : consumers) {
                consumer.start();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
//        printWriter.close(); //debug
    }

    public synchronized void performAction(Consumer consumer, int value) {
        printWriter.println("Consumer with name " + consumer.getConsumerName() + " read the value " + value);
//        printWriter.println(value);
        printWriter.flush();
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTopicName() {
        return topicName;
    }

    public MessageBroker getMessageBroker() {
        return messageBroker;
    }
}

