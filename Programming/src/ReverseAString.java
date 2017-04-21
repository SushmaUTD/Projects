import java.util.Scanner;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class ReverseAString {

	/**
	 * @param args
	 */
	public static void reverseWordsInAString(char[] s) {
		if (s == null)
			System.out.print("Empty String");
		else {
			int len = s.length;
			int start = 0;
			int checker = 0;
			for (int i = 0; i < len; i++) {
				if ((s[i] == ' ' || i + 1 == len) && checker > 0) {
					if (i + 1 == len)
						i++;
					reverseString(s, start, i);
					
					checker = 0;
					start = i + 1;
				} else if (s[i] == ' ' && checker == 0) {
					start++;
				} else
					checker++;
			}
			reverseString(s, 0, s.length);
			System.out.print(s);
		}
	}

	public static void reverseString(char[] s, int start, int end) {
		int len = start + (end - start) / 2;
		int j = end - 1;
		char temp;
		for (int i = start; i < len; i++, j--) {
			temp = s[i];
			s[i] = s[j];
			s[j] = temp;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner s = new Scanner(System.in);
		String input = s.nextLine();
		
		reverseWordsInAString(input.toCharArray());
	}

}
