package netdiscovery;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

import netdiscovery.PeerDiscovery.Peer;


public class DiscoveryEngine {

	private Broadcasts bc;
	private ArrayList<String> bl;
	private ArrayList<InetAddress> al;
	private ArrayList<InetAddress> pl;
	private PeerDiscovery mp;
	private int group = 6969;
	
	public DiscoveryEngine() throws IOException 
	{
		bc = new Broadcasts();
		bl = bc.getBroadcast();
		al = bc.getAddresses();
		pl = new ArrayList<InetAddress>();
		
	}
	public void startBroadcastListner() throws IOException
	{
		mp = new PeerDiscovery( group, 6969, bl);
        System.out.print( "Start Listening..." );
        //mp.disconnect();
        mp.startListen();
        
	}
	public ArrayList<InetAddress> getPeers() throws IOException
	{
		
		for(String iadd : bl)
		{
			System.out.println("Peer Discovery on broadcast address: " + iadd);
		}
		
		mp = new PeerDiscovery( group, 6969, bl);
		
	    mp.startListen();
        Peer[] prePeers = mp.getPeers( 3000, ( byte ) 0 );
        mp.disconnect();
        
        //System.out.println( prePeers.length + " peers found" );
        for( Peer p : prePeers )
        {
          
          if(!al.contains(p.ip))
          {
        	  pl.add(p.ip);
          }
          
        }
        
        return pl;
        /*
        if(pl.size() > 0)
        {
        	System.out.println("Peer Found");
        	for(InetAddress ip : pl)
        	{
        		System.out.println("\t" + ip.getHostAddress() );
                
        	}
        	//System.exit(0);
        }
        else
        {
        mp = new PeerDiscovery( group, 6969, bl);
        System.out.print( "Start Listening..." );
        //mp.disconnect();
        mp.startListen();
        }
        */
        /*
		 boolean stop = false;

	      BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) );

	      while( !stop )
	      {
	        System.out.println( "enter \"q\" to quit, or anything else to query peers" );
	        String s = br.readLine();

	        if( s.equals( "q" ) )
	        {
	          System.out.print( "Closing down..." );
	          mp.disconnect();
	          System.out.println( " done" );
	          stop = true;
	        }
	        else
	        {
	          System.out.println( "Querying" );

	          Peer[] peers = mp.getPeers( 3000, ( byte ) 0 );

	          System.out.println( peers.length + " peers found" );
	          for( Peer p : peers )
	          {
	            System.out.println( "\t" + p );
	          }
	        }
	      }
	    */
	}
	
	
}
