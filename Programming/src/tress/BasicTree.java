/**
 * 
 */
package tress;

/**
 * @author Sushma
 *
 */
class BinaryTree {
	int data;
	BinaryTree left;
	BinaryTree right;
}

public class BasicTree {

	/**
	 * @param args
	 */
	public static BinaryTree root;

	public static void insert(int data) {
		BinaryTree child = new BinaryTree();
		BinaryTree temp = new BinaryTree();
		child.data = data;
		child.left = null;
		child.right = null;
		if (root == null) {
			root = child;

			return;
		}
		temp = root;
		while (true) {
			if (temp.data >= data) {
				if (temp.left == null) {
					temp.left = child;
					return;
				} else
					temp = temp.left;
			} else {
				if (temp.right == null) {
					temp.right = child;
					return;
				} else
					temp = temp.right;
			}
		}
	}

	public static void displayTree(BinaryTree node) {
		if (node != null) {
			displayTree(node.left);
			System.out.println(node.data);
			displayTree(node.right);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int input[] = { 31, 56, 10, 45, 67, 8 };
		for (int i = 0; i < input.length; i++) {
			insert(input[i]);
		}
		displayTree(root);
	}

}
