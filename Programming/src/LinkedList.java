import java.util.ArrayList;
import java.util.List;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
class Node {
	Node next;
	int data;
};

public class LinkedList {
	static Node head1;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (int i = 0; i < 3; i++) {
			insertNode(i);
		}
		printList(head1);
		System.out.print("\n");
		
	}
	
	public static void findLoopInLinkedList(Node temp,int no_of_nodes)
	{
		int prev_sum=0;
		int jump_value=0;
		boolean shouldCheckEnd=false;
		if(temp!=null)
		{
			while(temp!=null)
			{
				jump_value=temp.data+prev_sum;
				if(no_of_nodes%jump_value == no_of_nodes)
					shouldCheckEnd=true;
				while(jump_value-- >= 0)
				{
					temp = temp.next;
					
					if(shouldCheckEnd)
						if(jump_value-1 == no_of_nodes)
							if(temp == null)
								System.out.print("No Loop Present");
							else
								System.out.print("Loop Exists");
				}
			}
			System.out.print("No Loop");
		}
		else{
			System.out.print("No List");
		}
	}

	
	public static void insertNode(int data) {
		Node newNode = new Node();
		newNode.data = data;
		newNode.next = null;
		
			if (head1 == null) {
				head1 = newNode;
			} else {
				Node temp = new Node();
				temp = head1;
				for (; temp.next != null;) {
					temp = temp.next;
				}
				temp.next = newNode;
			}
	}

	public static void printList(Node node) {
		while (node != null) {
			System.out.println(node.data);
			node = node.next;
		}
	}

	public static void deleteCurrentNode(Node node) {
		if (node.next == null) {
			node = null;

		} else {
			node.data = node.next.data;
			node.next = node.next.next;
		}

	}

	public static Node deleteNode(int data, Node head) {
		Node temp = new Node();
		Node prev = new Node();
		temp = head;
		for (; temp != null;) {
			if (temp.data == data) {
				prev.next = temp.next;
				break;
			}
			prev = temp;
			temp = temp.next;

		}
		return head;
	}

}
