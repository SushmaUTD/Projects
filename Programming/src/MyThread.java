class MyThread extends Thread {
	int myNum;
	static int count;

	MyThread(int i) {
		super();
		count++;
		myNum = i;
	}

	public void run(){
		System.out.println("Count is"+count +"MyNum is"+myNum);
}

	public static void main(String args[]) {
		for (int i = 0; i < 4; i++) {
			MyThread mt = new MyThread(i);
			mt.start();
		}
	}
}
