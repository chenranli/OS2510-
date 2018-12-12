package indexSearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Helper implements Runnable{
	private final int id;
	private final String keywords;
	private BlockingQueue<HashMap> q;
	private List<String> fileList;
	Helper(int id, String keywords,BlockingQueue q,List list){
		this.id  = id;
		this.keywords = keywords;
		this.q = q;
		this.fileList = list;
	}
	@Override
	public void run() {
		Search s = new Search();
		try {
			Object obj = s.search(keywords, id, fileList);
			HashMap<String,HashMap<Integer,Integer>> hm = new HashMap<String,HashMap<Integer,Integer>>();
			hm = (HashMap<String,HashMap<Integer,Integer>>) obj;
			//System.out.println(hm);
			try {
				if(!q.contains(hm)) {
					q.put(hm);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//Server.tmp.putAll(hm);
			LinkedHashSet<Integer> hs = new LinkedHashSet<Integer>();
			for(HashMap<Integer,Integer> value: hm.values()) {
				for(Integer v: value.keySet()) {
					hs.add(v);
				}
			}
			//System.out.println(hs);
			Server.ans.addAll(hs);
			//System.out.println(Server.ans);
			//Server.ans.addAll((HashSet)s.search(keywords, id));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
