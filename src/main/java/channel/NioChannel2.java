package channel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import plugincore.PluginEngine;
import shared.MsgEvent;
import shared.MsgEventType;




public class NioChannel2 {
    private static int PORT = 8000;
    //private static final long PAUSE_BETWEEEN_MSGS = 10; // millisecs
    private static ByteBuffer echoBuffer = ByteBuffer.allocate(4096);
    private static ConcurrentHashMap<Integer, SocketChannel> chm
                        = new ConcurrentHashMap<Integer, SocketChannel>();
    private static ConcurrentHashMap<String, Integer> ahm
    					= new ConcurrentHashMap<String, Integer>();
    private static ConcurrentHashMap<Integer,String> remainderHash 
    					= new ConcurrentHashMap<Integer,String>();
    
    //XML stuff
    private static JAXBContext jaxbContext;
    private static Unmarshaller LogEventUnmarshaller;
    private static Marshaller LogEventMarshaller;
    
    //connection stuff
    private static boolean isConnected = false;
    private static boolean isServer = false;
    private static String serverIp;
    private static int socketPort;
    private static int gatewayChannel = -1;
    
    //connection
    public static Selector selector;
    
    public NioChannel2(Boolean isServer,int socketPort, String serverIp) throws JAXBException, IOException  
    {
    	this.isServer = isServer;
    	this.socketPort = socketPort; 
    	this.serverIp = serverIp;
    	
    	//XML Stuff
    	jaxbContext = JAXBContext.newInstance(MsgEvent.class);
    	LogEventUnmarshaller = jaxbContext.createUnmarshaller();
    	LogEventMarshaller = jaxbContext.createMarshaller();
		LogEventMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		
		PluginEngine.nioChannelIsActive = true;
		
		try
		{
			if(isServer)
			{
				ServerConnectionManager();
			}
			else
			{
				ClientConnectionManager();
			}
			
			int connectionAttempts = 0;
			while(!isConnected)
			{
				connectionAttempts++;
				System.out.println("Connecting to server:" + serverIp + " port:" + socketPort);
				if(connectionAttempts>29)
				{
					throw new java.nio.channels.ConnectionPendingException();
				}
				Thread.sleep(1000);
			}
			msgTxClient();
			//msgRxClient();
			msgRxServer();
		}
		catch(Exception ex)
		{
			System.out.println("Could not start!! " + ex);
		}
        
        
    }        
    /**
     * This method sends messages to a random outPutStream
     * @return 
     */
    
    private static MsgEvent getMsgEvent(String message)
    {
    	MsgEvent me = null;
    	try{
    	
    	InputStream stream = new ByteArrayInputStream(message.getBytes());	
	    JAXBElement<MsgEvent> rootUm = LogEventUnmarshaller.unmarshal(new StreamSource(stream), MsgEvent.class);		        
	    me  = rootUm.getValue();
    	}
    	catch(Exception ex)
    	{
    		System.out.println("getMsg" + ex);
    		System.out.println("ppp:" + message + ":ppp");
 		   
    	}
    	return me;
    }
    
    private static String msgToXML(MsgEvent me)
    {
    	
    	String eventXml = null;
    	try
    	{
    	
		StringWriter LogEventXMLString = new StringWriter();
		QName qName = new QName("com.researchworx.cresco.shared", "LogEvent");
		JAXBElement<MsgEvent> root = new JAXBElement<MsgEvent>(qName, MsgEvent.class, me);
		LogEventMarshaller.marshal(root, LogEventXMLString);
		eventXml = LogEventXMLString.toString();
		}
    	catch(Exception ex)
    	{
    		System.out.println("genMsg " + ex);
    		//System.out.println(eventXml);
    	}
    	return eventXml;
    }
    
    private static void ClientConnectionManager() 
    {
        new Thread("client-connection-manager") 
        {
            public void run() 
            {
                try 
                {
                	 // Create a new selector
            		//Selector selector = Selector.open();
                	selector = Selector.open();
                    // Open a listener on each port, and register each one
                    SocketChannel ssc = SocketChannel.open();
                    //ssc.configureBlocking(true);
                    
                    //ssc.register(selector, SelectionKey.OP_CONNECT);
                    //ssc.connect(new InetSocketAddress("127.0.0.1", 8000));
                    //ssc.connect(new InetSocketAddress("127.0.0.1", 8000));
                    ssc.connect(new InetSocketAddress(serverIp, socketPort));
                    chm.put(ssc.hashCode(), ssc);
                    //register gateway
                    gatewayChannel = ssc.hashCode();
                    System.out.println("Connecting to " + PORT);
                    
                    while (!ssc.finishConnect()) 
                    {
                    	System.out.println("ClientConnectionManager: isConnected: " + ssc.isConnected() + " isConnectionPending:" + ssc.isConnectionPending() + " isOpen: " + ssc.isOpen());
                		
                    }
                    if(ssc.isConnected())
                    {
                    	ssc.configureBlocking(false);
                    	isConnected = true;
                    }
                    
                    ssc.register(selector, SelectionKey.OP_READ);
                    
                    
                    
                    int connectionError = 0;
                    while (PluginEngine.nioChannelIsActive) 
                    {
                    	
                    	if(!ssc.isConnected())
                    	{
                    		System.out.println("ClientConnectionManager: isConnected: " + ssc.isConnected() + " isConnectionPending:" + ssc.isConnectionPending() + " isOpen: " + ssc.isOpen());
                    			
                    		
                    		connectionError++;
                    		if(connectionError < 10)
                    		{
                    			ssc.connect(new InetSocketAddress(serverIp, socketPort));
                    		}
                    		else
                    		{
                    			PluginEngine.nioChannelIsActive = false;
                    			isConnected = false;
                    			System.out.println("ClientConnectionManager: Too many connection failures!");
                    		}
                    		
                    	}
                    	else
                    	{
                    		isConnected = true;
                    	}
                    	Thread.sleep(1000);   
                    }
                    
                    if(!ssc.isConnected())
                	{
                    	ssc.close();
                	}
                    
                } 
                catch (Exception e) 
                {
                    e.printStackTrace();
                    PluginEngine.nioChannelIsActive = false;
                }
                if(selector.isOpen())
                {
                	try {
						selector.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    System.out.println("ClientConnectionManager: Closed Selector");
                }
                
             System.out.println("ClientConnectionManager: Exit on isActive=false"); 
             
            }
        }.start();
    }
    
    private static void ServerConnectionManager() 
    {
        new Thread("server-connection-manager") 
        {
            public void run() 
            {
                try 
                {
                	// Create a new selector
                    selector = Selector.open();
                    // Open a listener on each port, and register each one
                    ServerSocketChannel ssc = ServerSocketChannel.open();
                    ssc.configureBlocking(false);
                    ServerSocket ss = ssc.socket();            
                    InetSocketAddress address = new InetSocketAddress(PORT);
                    ss.bind(address);
                    //registers ACCEPT
                    ssc.register(selector, SelectionKey.OP_ACCEPT);
                    System.out.println("Going to listen on " + PORT);
                    isConnected = true;
                    while(PluginEngine.nioChannelIsActive)
                    {
                    	Thread.sleep(5000);
                    }
                    
                } 
                catch (Exception e) 
                {
                    e.printStackTrace();
                    PluginEngine.nioChannelIsActive = false;
                }
                if(selector.isOpen())
                {
                	try {
						selector.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    System.out.println("ClientConnectionManager: Closed Selector");
                }
                
             System.out.println("ClientConnectionManager: Exit on isActive=false"); 
             
            }
        }.start();
    }
    
    private static void msgRxServer() 
    {
        new Thread("Message-receive") 
        {
            public void run() 
            {
                try 
                {
                	//String remainder = new String();
                    while (PluginEngine.nioChannelIsActive) 
                    {
                    	selector.select();
                        Set<SelectionKey> selectedKeys = selector.selectedKeys();
                        Iterator<SelectionKey> it = selectedKeys.iterator(); 
                        
                        String msg = new String();
                        String oldremainder = new String();
                        /*
                        if(remainder.length() > 0)
                        {
                        	msg += remainder;
                        	remainder = "";
                        }
                        */
                        while (it.hasNext()) 
                        {
                        	SelectionKey key = (SelectionKey) it.next();
                            int hashcode = key.hashCode();
                            	
                            
                            
                            
                            if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) 
                            {
                                // Accept the new connection
                                ServerSocketChannel sscNew = (ServerSocketChannel) key
                                        .channel();
                                SocketChannel sc = sscNew.accept();
                                sc.configureBlocking(false);
                                
                                // Add the new connection to the selector                    
                                sc.register(selector, SelectionKey.OP_READ);
                                // Add the socket channel to the list
                                chm.put(sc.hashCode(), sc);
                                
                                System.out.println("Added Socket:" + chm.size() + " " + sc.socket().getInetAddress().toString() + " " + sc.hashCode());
                                it.remove();
                            }
                            else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
                                // Read the data
                                SocketChannel sc = (SocketChannel) key.channel();            
                                
                                synchronized(remainderHash) {
                                	if(remainderHash.containsKey(hashcode))
                                    {
                                    	msg += remainderHash.get(hashcode);
                                    	oldremainder += remainderHash.get(hashcode);
                                    	//remainder = "";
                                    	remainderHash.remove(hashcode);
                                    }
                                    }
                                
                                try
                                {
                                	int code = 0;
                                	
                                	while ((code = sc.read(echoBuffer)) > 0) 
                                	{
                                		byte b[] = new byte[echoBuffer.position()];
                                		echoBuffer.flip();
                                		echoBuffer.get(b);
                                		msg+=new String(b, "UTF-8");
                                	}
                                echoBuffer.clear();
                                
                               String[] sstr = msg.split("<\\?xml version=\"1\\.0\" encoding=\"UTF-8\" standalone=\"yes\"\\?>\n");
                               
                               for(String str : sstr)
                               {
                            	   if(str.length() > 0)
                            	   {
                            		   
                            		   //String inputMsg = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + str;
                            		   if(!str.endsWith("</ns2:LogEvent>\n"))
                            		   {
                            			   //remainder = inputMsg;
                            			   synchronized(remainderHash) {
                                           	
                            			   //remainderHash.put(hashcode, remainderHash.get(hashcode) + str);
                            			   remainderHash.put(hashcode, str);
                            			   }
                            			   //System.out.println("Adding remainer: hashcode:" + hashcode + " input:" + inputMsg);
                            		   }
                            		   else
                            		   {
                            			   String inputMsg = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + str;
                                		   
                            			   MsgEvent me = getMsgEvent(inputMsg);
                            			   if(me != null)
                            			   {
                            				   if((me.getParam("src_region") != null))
                            				   {
                            					   String msgHash = null;
                            					   if(me.getParam("src_agent") != null)
                            					   {
                            						   msgHash = me.getParam("src_region") + "_" + (me.getParam("src_agent"));
                            					   }
                            					   else
                            					   {
                            						   msgHash = me.getParam("src_region");
                            					   }
                            					   
                            					   if(!ahm.containsKey(msgHash))
                            					   {
                            						   ahm.put(msgHash, sc.hashCode());
                            						   System.out.println("Added: *" + msgHash + "* code:" + sc.hashCode() + " size:" + ahm.size());
                            					   }
                            					   else
                            					   {
                            						   
                            						   int hashCode = sc.hashCode();
                            						   if(!ahm.get(msgHash).equals(hashCode))
                            						   {
                            							   ahm.put(msgHash, sc.hashCode());
                            						   }
                            					   }
                            					   
                            					   PluginEngine.msgInQueue.offer(me);
                            				   }
                            				   else
                            				   {
                            					   System.out.println("msgRxClient: src_region is null");
                            				   }
                            				   
                            			   }
                            			   else
                            			   {
                            				   System.out.println("hashcode: " + hashcode + " oldreminder: " + oldremainder + " input:" + inputMsg);
                            				   System.out.println("Null MESSAGE");
                            			   }
                            		   }
                            	   }
                               }
                                }
                                catch(Exception ex)
                                {
                                	System.out.println("msgRxClient 0: isActive:" + PluginEngine.nioChannelIsActive + " " + ex.toString() );
                                	
                                	/*
                                	if(!sc.isConnected())
                                	{
                                		for (Entry<String, Integer> entry : ahm.entrySet()) {
                                            if (entry.getValue().equals(sc.hashCode())) 
                                            {
                                                //System.out.println(entry.getKey());
                                            	ahm.remove(entry);
                                            }
                                        }
                                		chm.remove(sc.hashCode());
                                		sc.close();
                                		selectedKeys.remove(key);
                                	}
                                	*/
                                }
                               it.remove();
                            }
                        }
                        
                    }
                    System.out.println("msgRxClient: Exit on isActive = false");
                    
                } 
                catch (Exception e) 
                {
                    //e.printStackTrace();
                    System.out.println("msgRxClient 1: " + e.toString());
                	
                }
                
            	
            }
        }.start();
    }
    
    private static void msgTxClient() {
        new Thread("Send-to-Clients") {
            public void run() 
            {
                try 
                {
                    while (PluginEngine.nioChannelIsActive) 
                    {
                    	try 
                    	{
                    		MsgEvent me = PluginEngine.msgOutQueue.poll(); //get logevent
                    		if(me != null)
                    		{	
                    			if((me.getParam("dst_region") != null))
                    			{
                    				String msgHash = null;
                    				if(me.getParam("dst_agent") != null)
                    				{
                    					msgHash = me.getParam("dst_region") + "_" + (me.getParam("dst_agent"));
                    				}
                    				else
                    				{
                    					msgHash = me.getParam("dst_region");
                    				}
                    				
                    				if(ahm.containsKey(msgHash))
                    				{
                    					int socketHash = ahm.get(msgHash);
                    					
                    					if(chm.containsKey(socketHash))
                    					{
                    						SocketChannel sc = chm.get(socketHash);
                    						ByteBuffer buf = ByteBuffer.wrap((msgToXML(me)).getBytes());
                    						
                    						try
                    						{
                    							sc.write(buf);
                    						}
                    						catch(Exception ex)
                    						{
                    							sc.close();
                								chm.remove(socketHash);
                								ahm.remove(msgHash);
                    						}
                    					}
                    					
                    				}
                    				else
                    				{
                    					if(gatewayChannel != -1)
                    					{
                    						SocketChannel sc = chm.get(gatewayChannel);
                    						ByteBuffer buf = ByteBuffer.wrap((msgToXML(me)).getBytes());
                    						
                    						try
                    						{
                    							sc.write(buf);
                    						}
                    						catch(Exception ex)
                    						{
                    							sc.close();
                								chm.remove(gatewayChannel);
                								ahm.remove(msgHash);
                								gatewayChannel = -1;
                    						}
                    					}
                    					else
                    					{
                    						//removed for debuging
                    						//System.out.println("Route not found in table for : " + msgHash);
                        				}
                    				}
                    			}
                    			else
                    			{
                    				System.out.println("No dst_dst region!");
                    			}
                    		}
                    		else
                    		{
                    			Thread.sleep(10);
                    		}
     				   }
                    	catch(Exception ex)
                    	{
                    		ex.printStackTrace();
                    	}
                    }
                    System.out.println("msgTxClient: Exit on isActive = false");
                } 
                catch (Exception e) 
                {
                    e.printStackTrace();
                }
            }
        }.start();
    }

}