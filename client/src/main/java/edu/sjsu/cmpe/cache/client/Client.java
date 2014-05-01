package edu.sjsu.cmpe.cache.client;


import com.google.common.hash.HashFunction;
import com.google.common.hash.*;
import com.google.common.hash.HashCode;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Client {
	

	private static HashFunction hashFunction= Hashing.md5();
	private static SortedMap<Integer, String> circle =new TreeMap<Integer, String>();
	

	 public static void add(String node,int i) {
		 
		 HashCode hc=hashFunction.hashLong(i);
		 
		  circle.put(hc.asInt(),node);
	 }
	 
	  public void remove(String node) {

		  //circle.remove(node);
		  circle.remove(Hashing.md5().hashCode());
	  }

	 public static String get(Object key) {
	    if (circle.isEmpty()) {
	      return null;
	    }
	    int hash = hashFunction.hashLong((Integer)key).asInt();
	    if (!circle.containsKey(hash)) {
	      SortedMap<Integer, String> tailMap =circle.tailMap(hash);
	      hash = tailMap.isEmpty() ?
	             circle.firstKey() : tailMap.firstKey();
	    }
	    return circle.get(hash);
	  }


    public static void main(String[] args) throws Exception {
        
    	String value[]={"a","b","c","d","e","f","g","h","i","j"};
    	
    	System.out.println("Starting Cache Client...");
        
        String cacheserver1="http://localhost:3000";
        String cacheserver2="http://localhost:3001";
        String cacheserver3="http://localhost:3002";
        
        List<String> server=new ArrayList<String>();
        
        server.add(cacheserver1);
        server.add(cacheserver2);
        server.add(cacheserver3);
        
        
        
        
        
        for(int x=0; x<server.size(); x++)
        {

        	String x1=server.get(x);
        	System.out.println("String get " + x1);
        	add(x1,x);
        }
        
        for (int i=0;i<10;i++)
        {
        	int bucket = Hashing.consistentHash(Hashing.md5().hashLong(i),circle.size());
        	int key=i+1;
        	String new_server=get(bucket);
        	
        	System.out.println("server is : " + new_server);

        	CacheServiceInterface cacheserver = new DistributedCacheService(new_server);
        	
        	cacheserver.put(i+1, String.valueOf(value[i]));
		System.out.println("insertion start:");
		System.out.println("key is " + key);
		System.out.println("value is" + value[i]);
        	
        	        	
        }

	for (int i=0;i<10;i++)
        {
        	int bucket = Hashing.consistentHash(Hashing.md5().hashLong(i),circle.size());
        	int key=i+1;
        	String new_server=get(bucket);
        	
        	
        	CacheServiceInterface cacheserver = new DistributedCacheService(new_server);
        	
		cacheserver.get(i+1);
		System.out.println("getting starts");
		System.out.println("key is:" + key);
        	
        	System.out.println("Values is :"+cacheserver.get(i+1));
        	
        	//System.out.println("Cache Client.. ");
        	
        }

        
        
                
        
        
               
    }

}
