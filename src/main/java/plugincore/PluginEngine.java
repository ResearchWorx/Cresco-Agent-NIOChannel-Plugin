package plugincore;


import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import netdiscovery.DiscoveryEngine;

import org.apache.commons.configuration.SubnodeConfiguration;

import channel.NioChannel;
import shared.Clogger;
import shared.MsgEvent;
import shared.MsgEventType;
import shared.PluginImplementation;



public class PluginEngine {

	public static PluginConfig config;
	
	public static String pluginName;
	public static String pluginVersion;
	public static String plugin;
	public static String agent;
	public static String region;
	
	public static CommandExec commandExec;
	public static WatchDog wd;
	
	
	public static ConcurrentLinkedQueue<MsgEvent> msgInQueue;
	public static ConcurrentLinkedQueue<MsgEvent> msgOutQueue;
	
	public static HashMap<String,MsgEvent> rpcMap;
	//public static boolean watchDogActive = false; //agent watchdog on/off
	public static Clogger clog;

	public static boolean nioChannelIsActive;
	
	public PluginEngine()
	{
		pluginName="NIOPlugin";
		rpcMap = new HashMap<String,MsgEvent>();
	}
	public void shutdown()
	{
		System.out.println("Plugin Shutdown : Agent=" + agent + "pluginname=" + plugin);
		wd.timer.cancel(); //prevent rediscovery
		try
		{
			MsgEvent me = new MsgEvent(MsgEventType.CONFIG,region,null,null,"disabled");
			me.setParam("src_region",region);
			me.setParam("src_agent",agent);
			me.setParam("src_plugin",plugin);
			me.setParam("dst_region",region);
			
			//msgOutQueue.offer(me);
			msgInQueue.offer(me);
			//PluginEngine.rpcc.call(me);
			System.out.println("Sent disable message");
		}
		catch(Exception ex)
		{
			String msg2 = "Plugin Shutdown Failed: Agent=" + agent + "pluginname=" + plugin;
			clog.error(msg2);
			
		}
	}
	public String getName()
	{
		   return pluginName; 
	}
	public String getVersion() //This should pull the version information from jar Meta data
    {
		   String version;
		   try{
		   String jarFile = PluginImplementation.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		   File file = new File(jarFile.substring(5, (jarFile.length() -2)));
           FileInputStream fis = new FileInputStream(file);
           @SuppressWarnings("resource")
		   JarInputStream jarStream = new JarInputStream(fis);
		   Manifest mf = jarStream.getManifest();
		   
		   Attributes mainAttribs = mf.getMainAttributes();
           version = mainAttribs.getValue("Implementation-Version");
		   }
		   catch(Exception ex)
		   {
			   String msg = "Unable to determine Plugin Version " + ex.toString();
			   clog.error(msg);
			   version = "Unable to determine Version";
		   }
		   
		   return pluginName + "." + version;
	   }
	//steps to init the plugin
	public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue,ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region,String agent, String plugin)  
	{
		commandExec = new CommandExec();
		//this.msgOutQueue = msgOutQueue; //send directly to log queue
		this.msgInQueue = msgInQueue; //messages to agent should go here
		
		this.agent = agent;
		this.plugin = plugin;
		
		this.region = region;
		try{
			//no need for this plugin to use the log queue input
			/*
			if(msgOutQueue == null)
			{
				System.out.println("MsgOutQueue==null");
				return false;
			}
			*/
			msgOutQueue = new ConcurrentLinkedQueue<MsgEvent>(); //create our own queue
			
			if(msgInQueue == null)
			{
				System.out.println("MsgInQueue==null");
				return false;
			}
			
			this.config = new PluginConfig(configObj);
			
			//create logger
			clog = new Clogger(msgInQueue,region,agent,plugin); //send logs directly to outqueue
			
			String startmsg = "Initializing Plugin: Region=" + region + " Agent=" + agent + " plugin=" + plugin + " version" + getVersion();
			clog.log(startmsg);
			
			
	    	System.out.println("Starting NIOChannel Plugin");
	    	nioChannelIsActive = true;
	    	
	    	try{
	    		//establish AMPQ connectivity
	    		/*
	    		factory = new ConnectionFactory();
	    		factory.setHost(PluginEngine.config.getAMPQControlHost());
	    		factory.setUsername(PluginEngine.config.getAMPQControlUser());
	    		factory.setPassword(PluginEngine.config.getAMPQControlPassword());
	    		factory.setConnectionTimeout(10000);
	    		connection = factory.newConnection();
    	    
	    		Connection connection = factory.newConnection();
	    		//RPC CHANNEL
	    		RPC_CHANNEL_NAME = PluginEngine.region + "_control_" + PluginEngine.agent;
	    		rpc_channel = connection.createChannel();
	    		rpc_channel.queueDeclare(RPC_CHANNEL_NAME, false, false, false, null);
	    		rpc_channel.basicQos(1);
	    		//LOG CHANNEL
	    		//LOG_CHANNEL_NAME = PluginEngine.config.getRegion() + "_log";
	    		LOG_CHANNEL_NAME = region + "_log";
	    		log_channel = PluginEngine.connection.createChannel();
	    		log_channel.exchangeDeclare(LOG_CHANNEL_NAME, "fanout");
	    		*/
	    		int bufferSize = 1024;
	        	int tx_threads = 4;
	        	int rx_threads = 4;
	        	int nioPort = 8000;
	        	System.out.println("Starting Broadcast Discovery");
	        	DiscoveryEngine de = new DiscoveryEngine();
	        	ArrayList<InetAddress> pl = de.getPeers();
	        	/*
	        	if(args.length>2)
	        	{
	        		InetAddress addr = InetAddress.getByName(args[2]);
	        		pl.clear();
	        		pl.add(addr);
	        	}
	        	*/
	        	try
	        	{
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
	        	}
	        	catch(Exception ex)
	        	{
	        		System.out.println("Discovery Error: " + ex.toString());
	        		System.out.println("Trying local connection...");
	        		NioChannel nioc = new NioChannel(false, nioPort, "127.0.0.1",tx_threads,rx_threads,bufferSize);
	        	}
	    	}
	    	catch(Exception ex)
	    	{
	    		System.out.println("NIO Plugin Init error: " + ex.toString());
	    		return false;
	    	}
    		
			//Create Incoming log Queue wait to start
	    	/*
	    	AMPQRPCConsumer rc = new AMPQRPCConsumer();
    		RPCConsumerThread = new Thread(rc);
	    	RPCConsumerThread.start();
	    	while(!RPCConsumerEnabled)
	    	{
	    		Thread.sleep(1000);
	    		String msg = "Waiting for AMPQRPCConsumer Initialization : Region=" + region + " Agent=" + agent + " plugin=" + plugin;
	    		clog.log(msg);
	    	}
	    	*/
	    	/*
	    	AMPQLogProducer v = new AMPQLogProducer();
	    	ProducerThread = new Thread(v);
	    	ProducerThread.start();
	    	while(!ProducerEnabled)
	    	{
	    		Thread.sleep(1000);
	    		String msg = "Waiting for AMPQProducer Initialization : Region=" + region + " Agent=" + agent + " plugin=" + plugin;
	    		clog.log(msg);
	    	}
	    	*/
	    	/*
	    	AMPQLogConsumer lc = new AMPQLogConsumer();
    		LogConsumerThread = new Thread(lc);
	    	if(config.getLogConsumerEnabled())
	    	{
	    		LogConsumerThread.start();
	    		while(!LogConsumerEnabled)
		    	{
		    		Thread.sleep(1000);
		    		String msg = "Waiting for AMPQLogConsumer Initialization : Region=" + region + " Agent=" + agent + " plugin=" + plugin;
		    		clog.log(msg);
		    	}
	    		PluginEngine.LogConsumerActive = true;
		    	
		    	
	    	}
    		*/
	    	
	    	//PluginEngine.RPCConsumerActive = true;
	    	//PluginEngine.ProducerActive = true;
	    	
	    	
	    	//Establish Control Channel with other Agents
	    	//acc = new AMPQAgentControlChannel();
	    	
	    	wd = new WatchDog();
			
    		return true;
    		
		
		}
		catch(Exception ex)
		{
			String msg = "ERROR IN PLUGIN: : Region=" + region + " Agent=" + agent + " plugin=" + plugin + " " + ex.toString();
			clog.error(msg);
			return false;
		}
		
	}
	public static boolean enableLogConsumer(boolean enable) throws InterruptedException
	{
		try
		{
			/*
			PluginEngine.LogConsumerThread.start();
			while(!PluginEngine.LogConsumerEnabled)
			{
				Thread.sleep(1000);
				String msg = "Waiting for AMPQLogConsumer Initialization : Region=" + PluginEngine.region + " Agent=" + PluginEngine.agent + " plugin=" + PluginEngine.plugin;
				PluginEngine.clog.log(msg);
			}
			PluginEngine.LogConsumerActive = true;
			*/
			return true;
			
		}
		catch(Exception ex)
		{
			System.out.println("NIOChannel : PluginEngine : enableLog");
			return false;
		}
		
	}
	public void msgIn(MsgEvent me)
	{
		
		final MsgEvent ce = me;
		try
		{
		Thread thread = new Thread(){
		    public void run(){
		
		    	try 
		        {
					MsgEvent re = commandExec.cmdExec(ce);
					if(re != null)
					{
						re.setReturn(); //reverse to-from for return
						msgInQueue.offer(re); //send message back to queue
					}
					
				} 
		        catch(Exception ex)
		        {
		        	System.out.println("Controller : PluginEngine : msgIn Thread: " + ex.toString());
		        }
		    }
		  };
		  thread.start();
		}
		catch(Exception ex)
		{
			System.out.println("Controller : PluginEngine : msgIn Thread: " + ex.toString());        	
		}
		
	}
		
		
}