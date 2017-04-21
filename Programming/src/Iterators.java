import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;




public class Iterators implements Iterator{

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Integer> l1 = new ArrayList<Integer>();
		l1.add(1);
		l1.add(2);
		l1.add(3);
		List<Integer> l2 = new ArrayList<Integer>();
		l2.add(4);
		l2.add(5);
		l2.add(6);
		Iterator<Integer> it1,it2;
		it1= l1.iterator();
		it2=l2.iterator();
		List<Iterator> finalIterator = new ArrayList<Iterator>();
		finalIterator.add(it1);
		finalIterator.add(it2);
		
		for(Iterator Ite :finalIterator)
		{
			while(Ite.hasNext())
				System.out.print(Ite.next());
		}
	}

	@Override
	public boolean hasNext() {
		
		return false;
	}

	@Override
	public Object next() {
		return null;
	}


}
