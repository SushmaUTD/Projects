public class Stmt
{
	public static void main(String[] args) {
		m(3, 2);
	}
	public static int m(int a, int b) {
		if (a < 2)
			a++; else b++;
		return a + b;
	}
}
