import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class Encoding {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		@SuppressWarnings("resource")
		Scanner scan = new Scanner(System.in);
		Map<String, String> alphaMap = new HashMap<String, String>();
		int inputLength = Integer.parseInt(scan.nextLine());
		String inputString = scan.nextLine();
		for (int i = 0; i < inputLength; i++) {
			if (i < (inputLength - 1))
				alphaMap.put(inputString.charAt(i) + "",
						inputString.charAt(i + 1) + "");
			else
				alphaMap.put(inputString.charAt(i) + "", inputString.charAt(0)
						+ "");
		}
		int no_of_inputs = Integer.parseInt(scan.nextLine());
		List<String> inputs = new ArrayList<String>();
		for (int i = 0; i < no_of_inputs; i++) {
			inputs.add(scan.nextLine());
		}

		for (int i = 0; i < no_of_inputs; i++) {
			for (int j = 0; j < inputs.get(i).length(); j++) {
				if (alphaMap.get(inputs.get(i).charAt(j) + "") != null)
					System.out
							.print(alphaMap.get(inputs.get(i).charAt(j) + ""));
				else
					System.out.print(inputs.get(i).charAt(j) + "");
			}
		}

	}

}
