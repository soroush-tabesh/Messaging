package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TopicWriter {

    private final Map<String, Transaction> transactions;
    private final RandomAccessFile buffer;
    private final Topic topic;
    private final Logger logger = Logger.getInstance();
    private final Lock putLock = new ReentrantLock(false); //prevent thread starvation

    TopicWriter(Topic topic) {
        RandomAccessFile buffer1;
        this.topic = topic;
        transactions = new HashMap<>();
        try {
            buffer1 = new RandomAccessFile(topic.getTopicFile(), "rws");
        } catch (FileNotFoundException e) {
            buffer1 = null;
            logger.log("Error accessing topic file '" + topic.getTopicFile() + "'"
                    , Logger.Severity.ERROR);
        }
        buffer = buffer1;
    }

    public void put(String producerName, int value) {
        putLock.lock();
        try {
            if (value <= 0) {
                handleTransactionOperation(producerName, value);
            } else {
                handleInsertOperation(producerName, value);
            }
        } finally {
            putLock.unlock();
        }
    }

    private void handleTransactionOperation(String producerName, int value) {
        switch (value) {
            case 0:
                startTransaction(producerName);
                break;
            case -1:
                commitTransaction(producerName);
                break;
            case -2:
                cancelTransaction(producerName);
        }
    }

    private void handleInsertOperation(String producerName, int value) {
        if (transactions.containsKey(producerName)) {
            transactions.get(producerName).put(value);
        } else {
            writeValue(value);
        }
    }

    private void addTransaction(String producerName) {
        transactions.put(producerName, new Transaction(this, producerName));
    }

    /**
     * This method is used to start a transaction for putting a transaction of values inside the buffer.
     *
     * @return Nothing.
     */
    private void startTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            logger.log("Request for duplicate transaction. Committing underlying transaction and starting" +
                    " a new one for'" + producerName + "'", Logger.Severity.WARNING);
            commitTransaction(producerName);
        }
        addTransaction(producerName);
    }

    /**
     * This method is used to end the transaction for putting a its values inside the file.
     *
     * @return Nothing.
     */
    private void commitTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            transactions.get(producerName).commit();
            transactions.remove(producerName);
        } else {
            logger.log("No active transaction for '" + producerName + "' to commit.", Logger.Severity.ERROR);
        }
    }

    /**
     * This method is used to cancel a transaction.
     *
     * @return Nothing.
     */
    private void cancelTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            transactions.remove(producerName);
        } else {
            logger.log("No active transaction for '" + producerName + "' to close.", Logger.Severity.ERROR);
        }
    }

    private void writeValue(int value) {
        try {
            buffer.write(String.format("%d\n", value).getBytes());
        } catch (IOException e) {
            logger.log("Problem in writing to topic '" + topic.getName() + "'.", Logger.Severity.ERROR);
//            e.printStackTrace();
        }
    }

    private static class Transaction {
        private final TopicWriter topicWriter;
        private final Queue<Integer> values;
        private final String producerName;

        Transaction(TopicWriter topicWriter, String producerName) {
            this.topicWriter = topicWriter;
            this.producerName = producerName;
            values = new LinkedList<>();
        }

        void put(int value) {
            values.add(value);
        }

        void commit() {
            topicWriter.writeValue(0);
            while (!values.isEmpty()) {
                topicWriter.writeValue(values.remove());
            }
            topicWriter.writeValue(-1);
        }
    }
}
