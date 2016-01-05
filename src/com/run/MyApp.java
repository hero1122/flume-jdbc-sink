package com.run;

public class MyApp {
	
	public static void main(String[] args) {
	    MyRpcClientFacade client = new MyRpcClientFacade();
	    // Initialize client with the remote Flume agent's host and port
	    client.init("101.231.101.140", 4141);

	    // Send 10 events to the remote Flume agent. That agent should be
	    // configured to listen with an AvroSource.
//	    String sampleData = "Hello Flume!";
	    String sampleData = " 0000	88	1429732570181	2015-04-22T18:22:02.000Z	54-E6-FC-1A-F1-A4	00:03:7F:00:00:00	51012200000002";
	    for (int i = 0; i < 10; i++) {
	      client.sendDataToFlume(sampleData);
	    }

	    client.cleanUp();
	  }
}
