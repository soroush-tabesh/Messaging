package Main;

import Broker.Logger;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Logger.getInstance().log("salam");
        Logger.getInstance().log("tokhmi", Logger.Severity.WARNING);
        //new Program(args).run();
    }
}
