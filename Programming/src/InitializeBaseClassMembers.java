/**
 * 
 */

/**
 * @author Sushma
 *
 */
class BaseClass{
	int a,b;
	BaseClass(int a,int b)
	{
		this.a =a;
		this.b=b;
	}
	
	public void printValues(int a,int b)
	{
		System.out.print(a+"-"+b);
	}
}
public class InitializeBaseClassMembers extends BaseClass {

	InitializeBaseClassMembers(int a, int b) {
		super(a, b);
		// TODO Auto-generated constructor stub
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

/*
 * #include <iostream>
using namespace std;

class A{
	protected:
	int a;
	int b;
	public:
	void setA(int ab)
	{
		a=ab;
	}
	void setB(int bb)
	{
		b=bb;
	}
};
class B:public A
{
	public:
	int ab;
	int bb;
	B(int ab,int bb)
	{
		setA(ab);
		setB(bb);
	}
	public:
	void printValues()
	{
		cout<<a<<"\n"<<b;
	}
};
int main() {
	// your code goes here
	B b(1,5);
	b.printValues();
	return 0;
}
 */


