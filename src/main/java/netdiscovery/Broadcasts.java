package netdiscovery;


import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;

public class Broadcasts
{
	private ArrayList<String> listOfBroadcasts = null;
	private ArrayList<InetAddress> listOfAddresses = null;
	
	public Broadcasts()
	{
		listOfBroadcasts = new ArrayList<String>();
		listOfAddresses = new ArrayList<InetAddress>();
		
	}
	
	public ArrayList<InetAddress> getAddresses(){
	    String found_bcast_address=null;
	     System.setProperty("java.net.preferIPv4Stack", "true"); 
	        try
	        {
	          Enumeration<NetworkInterface> niEnum = NetworkInterface.getNetworkInterfaces();
	          while (niEnum.hasMoreElements())
	          {
	            NetworkInterface ni = niEnum.nextElement();
	            if(!ni.isLoopback()){
	                for (InterfaceAddress interfaceAddress : ni.getInterfaceAddresses())
	                {

	                  if(interfaceAddress != null)
	                  {
	                	  //found_bcast_address = interfaceAddress.getBroadcast().toString();
	                	  //found_bcast_address = found_bcast_address.substring(1);
	                	  //System.out.println("Found broadcast: " + found_bcast_address);
	                	  try{
	                	  //System.out.println("Found address: " + interfaceAddress.getAddress().getHostAddress().toString());
	                	  //System.out.println("Found mask: " + interfaceAddress.getNetworkPrefixLength());
	                	  //String broadcast=interfaceAddress.getAddress().getHostAddress().toString();
	                	  InetAddress address = interfaceAddress.getAddress();
	                	  //String subnet="192.168.0.1/29";
	              		  		
	              		if(!listOfAddresses.contains(address))
                    	{
                    		listOfAddresses.add(address);
                    	}
	              		
	              		//System.out.printf("Cody Broadcast Address:\t\t%s\t[%s]\n",info.getBroadcastAddress(),Integer.toBinaryString(info.asInteger(info.getBroadcastAddress())));
	                	  }
	                	  catch(Exception ex)
	                	  {
	                		  System.out.println(ex.toString());
	                	  }
	                	  /*
	                	  if(interfaceAddress.getBroadcast() != null)
	                	  {
	                	  System.out.println("Found broadcast: " + interfaceAddress.getBroadcast().toString());
	                	  }
	                	  */
	                  }
	                }
	            }
	          }
	          //return listOfBroadcasts.add(broadcast);
	        }
	        catch (SocketException e)
	        {
	          e.printStackTrace();
	        }
	        return listOfAddresses;
	        //return found_bcast_address;
	}
	
	public ArrayList<String> getBroadcast(){
	    String found_bcast_address=null;
	     System.setProperty("java.net.preferIPv4Stack", "true"); 
	        try
	        {
	          Enumeration<NetworkInterface> niEnum = NetworkInterface.getNetworkInterfaces();
	          while (niEnum.hasMoreElements())
	          {
	            NetworkInterface ni = niEnum.nextElement();
	            if(!ni.isLoopback()){
	                for (InterfaceAddress interfaceAddress : ni.getInterfaceAddresses())
	                {

	                  if(interfaceAddress != null)
	                  {
	                	  //found_bcast_address = interfaceAddress.getBroadcast().toString();
	                	  //found_bcast_address = found_bcast_address.substring(1);
	                	  //System.out.println("Found broadcast: " + found_bcast_address);
	                	  try{
	                	  //System.out.println("Found address: " + interfaceAddress.getAddress().getHostAddress().toString());
	                	  //System.out.println("Found mask: " + interfaceAddress.getNetworkPrefixLength());
	                	  String subnet=interfaceAddress.getAddress().getHostAddress().toString() + "/" + Short.toString(interfaceAddress.getNetworkPrefixLength());
	                	  //String subnet="192.168.0.1/29";
	              		SubnetUtils utils=new SubnetUtils(subnet);
	              		SubnetInfo info=utils.getInfo();
	              		
	              		String broadcast = info.getBroadcastAddress();
	    	              		
	              		if(!listOfBroadcasts.contains(broadcast))
                    	{
                    		listOfBroadcasts.add(broadcast);
                    	}
	              		
	              		//System.out.printf("Cody Broadcast Address:\t\t%s\t[%s]\n",info.getBroadcastAddress(),Integer.toBinaryString(info.asInteger(info.getBroadcastAddress())));
	                	  }
	                	  catch(Exception ex)
	                	  {
	                		  System.out.println(ex.toString());
	                	  }
	                	  /*
	                	  if(interfaceAddress.getBroadcast() != null)
	                	  {
	                	  System.out.println("Found broadcast: " + interfaceAddress.getBroadcast().toString());
	                	  }
	                	  */
	                  }
	                }
	            }
	          }
	          //return listOfBroadcasts.add(broadcast);
	        }
	        catch (SocketException e)
	        {
	          e.printStackTrace();
	        }
	        return listOfBroadcasts;
	        //return found_bcast_address;
	}
	
}
