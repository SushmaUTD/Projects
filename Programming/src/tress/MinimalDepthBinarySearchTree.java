/**
 * 
 */
package tress;

/**
 * @author Sushma
 *
 */
class BinaryNode{
	int data;
	BinaryNode left;
	BinaryNode right;
}
public class MinimalDepthBinarySearchTree {

	/**
	 * @param args
	 */
	public static BinaryNode insertWithMinimalDepth(int input[],int  start, int end)
	{
		BinaryNode temp = new BinaryNode();
		if(end<start)
		{
			return null;
		}
		else{
			int middle = (start+end)/2;
			temp.data=input[middle];
			temp.left=insertWithMinimalDepth(input, start, middle-1);
			temp.right=insertWithMinimalDepth(input, middle+1, end);
			return temp;
		}
	}
	
	public static void displayTree(BinaryNode node) {
		if (node != null) {
			displayTree(node.left);
			System.out.println(node.data);
			displayTree(node.right);
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int input[] = { 8, 9, 10, 11, 12, 13 };
		BinaryNode root = insertWithMinimalDepth(input, 0, input.length-1);
		if(root == null)
			System.out.print("Some issue");
		else
			displayTree(root);
			
	}

}
