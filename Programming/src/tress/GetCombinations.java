/**
 * 
 */
package tress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Sushma
 *
 */
public class GetCombinations {

	/**
	 * @param args
	 */
	//static declaration of the numbers and the alphabets
	public static Map<Integer,String> alphabetMap = new HashMap<Integer, String>(); 

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Keep all the alphabets corresponding to the numbers.(Since this is a traditional pattern and wont change)
		alphabetMap.put(1,"a,b,c");
		alphabetMap.put(2,"d,e,f");
		alphabetMap.put(3,"g,h,i");
		alphabetMap.put(4,"j,k,l");
		alphabetMap.put(5,"m,n,o");
		alphabetMap.put(6,"p,q,r");
		alphabetMap.put(7,"s,t,u");
		alphabetMap.put(8,"v,w,x");
		alphabetMap.put(9,"y,z");
		int[] values = {1,2,3};
		char[][] characters = {{'a','b','c'},{'d','e','f'},{'g','h','i'}}; 
		findCombinations(characters, 2,0);
	}
	
	public static String findCombinations(char[][] values,int index,int val)
	{
		if(index<0)
			return "";
		System.out.println(values[0][val]+""+values[index][0]);
		//return findCombinations(values, index);
		return "";
	}

}
