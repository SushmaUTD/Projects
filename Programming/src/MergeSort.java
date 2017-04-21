/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class MergeSort {

	/**
	 * @param args
	 */
	public static void mergeSort(int[] numbers, int low, int high) {
		int middle = low + (high - low) / 2;
		if (low < high) {
			mergeSort(numbers, low, middle);
			mergeSort(numbers, middle + 1, high);
			merge(numbers, low, high, middle);
		}
	}

	public static void merge(int[] numbers, int low, int high, int middle) {
		int[] helper=new int[high];
		for (int i = low; i < high; i++) {
			helper[i] = numbers[i];
		}
		int i = low;
		int j = middle + 1;
		int k = low;
		// Copy the smallest values from either the left or the right side back
		// to the original array
		while (i <= middle && j < high) {
			if (helper[i] <= helper[j]) {
				numbers[k] = helper[i];
				i++;
			} else {
				numbers[k] = helper[j];
				j++;
			}
			k++;
		}
		while (i <= middle) {
			numbers[k] = helper[i];
			k++;
			i++;
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int[] numbers = { 1, 5, 2, 66, 3, 8, 90 };
		mergeSort(numbers, 0, 7);
		for(int z=0;z<numbers.length;z++)
			System.out.println(numbers[z]);
	}

}
