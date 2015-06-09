package shared;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

@XmlRootElement
public class MsgEvent {

	  //private String msgType;
	  private MsgEventType msgType;
	  private String msgRegion;
	  private String msgAgent;
	  private String msgPlugin;
	  private Map<String,String> params;
	  
	  public MsgEvent()
	  {
		  
	  }
	  public MsgEvent(MsgEventType msgType, String msgRegion, String msgAgent, String msgPlugin, Map<String,String> params)
	  {
		  this.msgType = msgType;
		  //this.mType = MsgEventType.CONFIG;
		  this.msgRegion = msgRegion;
		  this.msgAgent = msgAgent;
		  this.msgPlugin = msgPlugin;
		  this.params = params;
		  this.params = new HashMap<String,String>(params);
		  
	  }
	  public MsgEvent(MsgEventType msgType, String msgRegion, String msgAgent, String msgPlugin, String msgBody)
	  {
		  this.msgType = msgType;
		  //this.mType = MsgEventType.CONFIG;
		  this.msgRegion = msgRegion;
		  this.msgAgent = msgAgent;
		  this.msgPlugin = msgPlugin;
		  this.params = new HashMap<String,String>();
		  params.put("msg", msgBody);
	  }
	  public String getMsgBody()
	  {
		  return params.get("msg");
	  }
	  public void setMsgBody(String msgBody)
	  {
		 params.put("msg", msgBody);
	  }
	  
	  @XmlJavaTypeAdapter(MsgEventTypesAdapter.class)
	  public  MsgEventType getMsgType() {
	        return msgType;
	  } 
	  public void setMsgType(MsgEventType msgType) {
	        this.msgType = msgType;
	  }
	  public String getMsgRegion()
	  {
		  return msgRegion;
	  }
	  public void setMsgRegion(String msgRegion)
	  {
		  this.msgRegion = msgRegion;
	  }
	  public String getMsgAgent()
	  {
		  return msgAgent;
	  }
	  public void setMsgAgent(String msgAgent)
	  {
		  this.msgAgent = msgAgent;
	  }
	  public String getMsgPlugin()
	  {
		  return msgPlugin;
	  }
	  public void setMsgPlugin(String msgPlugin)
	  {
		  this.msgPlugin = msgPlugin;
	  }
	  
	  
	  @XmlJavaTypeAdapter(MsgEventParamsAdapter.class)
	  public Map<String,String> getParams()
	  {
		  return params;
	  }
	  public void setParams(Map<String,String> params)
	  {
		  this.params = params;
	  }
	  public String getParam(String key)
	  {
		if(params.containsKey(key))
		{
			return params.get(key);
		}
		return null;
	  }
	  public void setParam(String key, String value)
	  {
		  params.put(key, value);
	  }
	  public void removeParam(String key)
	  {
		  params.remove(key);
	  }
	  public String getParamsString() 
	  {
		 
		  Map<String,String> tmpMap = new HashMap<String,String>(params);
		  //tmpMap.keySet().removeAll(params.keySet());
		  //params.putAll(tmpMap);
		  //target.putAll(tmp);
		  
		  StringBuilder sb = new StringBuilder();
		  
		    Iterator it = tmpMap.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pairs = (Map.Entry)it.next();
		        sb.append(pairs.getKey() + " = " + pairs.getValue() + "\n");
		        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
		        it.remove(); // avoids a ConcurrentModificationException
		    }
		    
		  return sb.toString();
		}
	    	  
	}