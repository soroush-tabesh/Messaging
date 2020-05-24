package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicReader {

    private final Topic topic;
    private final String groupName;
    private final RandomAccessFile topicFile;
    private final Logger logger = Logger.getInstance();
    private final Lock getLock = new ReentrantLock(true); //prevent thread starvation

    TopicReader(Topic topic, String groupName) {
        RandomAccessFile topicFile1;
        this.topic = topic;
        this.groupName = groupName;
        try {
            topicFile1 = new RandomAccessFile(topic.getTopicFile(), "rws");
        } catch (FileNotFoundException e) {
            topicFile1 = null;
            logger.log("Error accessing topic file '" + topic.getTopicFile().getName() + "'"
                    , Logger.Severity.ERROR);
        }
        topicFile = topicFile1;
    }

    public int get(String consumerName) {
        int value;
        getLock.lock();
        try {
            value = readValue();
            if (value <= 0) {
                handleTransactionOperation(value);
                return get(consumerName);
            } else {
                return value;
            }
        } finally {
            getLock.unlock();
        }
    }

    private int readValue() {
        waitForNext();
        try {
            return Integer.parseInt(topicFile.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -3;
    }

    private void waitForNext() {
        try {
            while (topicFile.read() == -1) ;
            topicFile.seek(topicFile.getFilePointer() - 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleTransactionOperation(int value) {
        switch (value) {
            case 0:
                startTransaction();
                break;
            case -1:
                stopTransaction();
        }
    }

    private void startTransaction() {
        getLock.lock();
    }

    private void stopTransaction() {
        getLock.unlock();
    }
}
