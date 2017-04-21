import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class SortHashTable {

	/**
	 * @param args
	 */
	public static void sortHashTable(Map<Integer,Integer> map)
	{
		List<Entry<Integer,Integer>> valueList = new ArrayList<Entry<Integer,Integer>>(map.entrySet());
		Collections.sort(valueList,new Comparator<Map.Entry<Integer, Integer>>() {

			@Override
			public int compare(Entry<Integer, Integer> o1,
					Entry<Integer, Integer> o2) {
				
				return o1.getValue().compareTo(o2.getValue());
			}
		});
		for(Entry<Integer, Integer> valueEntry: valueList)
		{
			System.out.println(valueEntry.getKey()+"-"+valueEntry.getValue());
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<Integer,Integer> map = new HashMap<Integer, Integer>();
		map.put(1, 5);
		map.put(3, 7);
		map.put(5, 8);
		map.put(4, 10);
		sortHashTable(map);
	}
}
