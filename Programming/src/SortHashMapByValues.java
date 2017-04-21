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
public class SortHashMapByValues {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String,Integer> values = new HashMap<String,Integer>();
		values.put("a", 5);
		values.put("b", 4);
		values.put("c", 3);
		values.put("d", 2);
		values.put("e", 1);
		List<Entry<String,Integer>> valueList = new ArrayList<Entry<String,Integer>>(values.entrySet());
		Collections.sort(valueList,new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1,
					Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		for(Entry<String, Integer> entry: valueList)
			System.out.println(entry.getKey()+entry.getValue());
	}
}
