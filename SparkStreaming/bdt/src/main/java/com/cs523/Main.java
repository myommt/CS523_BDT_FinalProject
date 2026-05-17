package com.cs523;

public class Main {
    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromArgs(args);
        EVStreamingToHiveApp app = new EVStreamingToHiveApp(config);
        app.run();
    }
}