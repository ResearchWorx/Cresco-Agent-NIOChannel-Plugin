package plugincore;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import netdiscovery.DiscoveryEngine;
import shared.MsgEvent;
import shared.MsgEventType;
import channel.NioChannel;


public class PluginEngine {
    
	public static ConcurrentLinkedQueue<MsgEvent> msgInQueue;
	public static ConcurrentLinkedQueue<MsgEvent> msgOutQueue;
    
	 
	 private static int high;
	 private static int low;
	 
	 
	public static boolean nioChannelIsActive;
	
	//private static ConcurrentHashMap<String,NioChannel> peerhm;
	   
    public static void main(String args[]) throws Exception 
    {
    	
    	//for testing
    	high = Integer.parseInt(args[1]);
    	low = Integer.parseInt(args[0]);
    	
    	
    	msgInQueue = new ConcurrentLinkedQueue<MsgEvent>();
    	msgOutQueue = new ConcurrentLinkedQueue<MsgEvent>();
    	
    	//peerhm = new ConcurrentHashMap<String,NioChannel>();
    	nioChannelIsActive = false;
    	
    	int bufferSize = 1024;
    	int tx_threads = 4;
    	int rx_threads = 4;
    	int nioPort = 8000;
    	System.out.println("Starting Broadcast Discovery");
    	DiscoveryEngine de = new DiscoveryEngine();
    	ArrayList<InetAddress> pl = de.getPeers();
    	if(args.length>2)
    	{
    		InetAddress addr = InetAddress.getByName(args[2]);
    		pl.clear();
    		pl.add(addr);
    	}
    	
    	if(pl.size()==0)
    	{
    		System.out.println("Starting Broadcast Discovery Listner");
    		de.startBroadcastListner();
    		System.out.println("Starting NioChannel Server on port: " + nioPort);
    		NioChannel nioc = new NioChannel(true, nioPort, "127.0.0.1",tx_threads,rx_threads,bufferSize);
    		
    	}
    	else
    	{
    		for(InetAddress address : pl)
    		{
    			if(!nioChannelIsActive)
    			{
    				//System.out.println("Connecting to server:" + address.getHostAddress() + " port:" + nioPort);
    				NioChannel nioc = new NioChannel(false, nioPort, address.getHostAddress(),tx_threads,rx_threads,bufferSize);
    			}
    		}
    	}
    	
    	
    	msgTxClient();
        msgRxClient();
    	
    	
    }
    
    private static void msgTxClient() {
        new Thread("TestSend") {
            public void run() 
            {
                try 
                {
                	int msgId = low;
                	int msgCount = 0;
                    while (nioChannelIsActive) 
                    {
                    	msgId++;
                    	msgCount++;
                    	if(msgId > high)
                    	{
                    		msgId = low;
                    	}
                    	
                    	MsgEvent me = new MsgEvent(MsgEventType.CONFIG,"region_name",null,null,"disabled");
                		me.setParam("src_region","region0");
                		me.setParam("src_agent","agent0_client" + msgId);
                		me.setParam("src_plugin","plugin0");
                		me.setParam("dst_region","region0");
                		me.setParam("dst_agent","agent0_client" + msgId);
                		me.setParam("dst_plugin","plugin0");
                		
                		PluginEngine.msgOutQueue.offer(me);
                		if(msgCount == 3000)
                		{
                			Thread.sleep(1500);
                			msgCount = 0;
                		}
                    }
                }
                catch(Exception ex)
                {
                	
                }
            }
          }.start();
        
    }

    private static void msgRxClient() {
        new Thread("TestRead") {
            public void run() 
            {
                try 
                {
                	long startIncoming = System.currentTimeMillis();
                    int incomingCount = 0;
                    
                	while (nioChannelIsActive) 
                    {
                		MsgEvent me = PluginEngine.msgInQueue.poll();
                		if(me != null)
                		{
                			//System.out.println("Message")
                			incomingCount++;
                			if(incomingCount == 10000)
         				   {
         					   float elapsed = (System.currentTimeMillis() - startIncoming)/1000;
         					   float mps = incomingCount/elapsed;
         					   System.out.println("Elapsed time in sec: " + elapsed + " Incoming message count:" + incomingCount + " Message per sec:" + mps);
         					   incomingCount = 0;
         					   startIncoming = System.currentTimeMillis();
         				   }
                		}
                		else
                		{
                		//	Thread.sleep(500);
                		}
                		//PluginEngine.msgOutQueue.offer(me);
                		
                    }
                }
                catch(Exception ex)
                {
                	
                }
            }
          }.start();
        
    }

}