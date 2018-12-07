package indexSearch;

import java.util.HashMap;
import java.util.HashSet;

public class Test {
	public static void main(String[] args) {
//		HashSet<Integer> s = new HashSet<Integer>();
//		s.add(1);
//		HashSet<Integer> s2 = new HashSet<Integer>();
//		s.addAll(s2);
//		System.out.print(s);
//		String[] s = new String[2];
//		s[0] = "a";
//		s[1] = "b";
//		for(String tmp: s) {
//			System.out.println(tmp);
//		}
		//HashSet<Integer> res = new HashSet<Integer>();
		//res.addAll(1);
		HashMap<Integer,Integer> count = new HashMap<Integer,Integer>();
		count.put(1, 2);
		count.put(2, 4);
		HashMap<String,HashMap<Integer,Integer>> ans = new HashMap<String,HashMap<Integer,Integer>>();
		ans.put("here", count);
		System.out.println(ans);
	}
	
	
}
