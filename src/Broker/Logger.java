package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class Logger {
    private static final Logger instance = new Logger();

    private final RandomAccessFile file;

    private Logger() {
        RandomAccessFile file1;
        try {
            file1 = new RandomAccessFile("./records.log", "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            file1 = null;
        }
        file = file1;
    }

    public static Logger getInstance() {
        return instance;
    }

    public synchronized void log(String message) {
        log(message, Severity.INFO);
    }

    public synchronized void log(String message, Severity severity) {
        try {
            file.seek(file.length());
            file.write(String.format("%s : %-8s : %s\n", new Date(), severity, message)
                    .getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {
        }
    }

    public enum Severity {
        INFO, WARNING, ERROR
    }
}
