import java.util.HashMap;
import java.util.Map;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class FindPairs {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int[] input = { 1,1,1,1 };

		System.out.print("No Of Pairs" + findPairs(input, 2));
	}

	public static int findPairs(int[] numbers, int k) {
		int count = 0;
		Map<Integer, Integer> tempMap = new HashMap<Integer, Integer>();
		for (int i = 0; i < numbers.length; i++) {
			if (tempMap.get(numbers[i]) == null)
				tempMap.put(numbers[i], 1);
			else
				tempMap.put(numbers[i], tempMap.get(numbers[i]) + 1);
		}
		for (int i = 0; i < numbers.length; i++) {
			if (tempMap.get(k - numbers[i]) != null) {
				if (tempMap.get(k - numbers[i]) > 0) {
					if (k - numbers[i] != numbers[i]) {
						count++;
						tempMap.put(k - numbers[i], 0);
						tempMap.put(numbers[i], 0);
					} else {
						if(tempMap.get(k - numbers[i]) > 1)
						{
							count++;
							tempMap.put(numbers[i], (tempMap.get(k - numbers[i])-1));
						}
					}
					
				}
			}
		}
		return count;
	}

}
