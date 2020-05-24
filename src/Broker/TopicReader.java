package Broker;

import java.io.RandomAccessFile;

public class TopicReader {

    private final Topic topic;
    private final String groupName;
    private /*final*/ RandomAccessFile topicFile;

    TopicReader(Topic topic, String groupName) {
        this.topic = topic;
        this.groupName = groupName;
        //To Do - Generate topicFile
    }

    public int get(String consumerName) {
        int value = 0;
        //To Do - Read next value from topicFile and return the value
        //To Do - Handle the transaction constraints
        return value;
    }
}
