import java.util.Arrays;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class MaximumDifferenceInAnArray {

	/**
	 * @param args
	 */
	public static void sort(int[] numbers) {
		Arrays.sort(numbers);
	}

	public static void findMaximumDifference(int[] numbers) {
		if (numbers == null) {
			System.out.print("Invalid Array");
		} else if (numbers.length == 2) {
			System.out.print(Math.abs(numbers[0] - numbers[1]));
		} else {
			sort(numbers);
			System.out.print("Maximum difference"
					+ (Math.abs(numbers[numbers.length - 1] - numbers[0])));
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int[] numbers = { -1, -9 };
		findMaximumDifference(numbers);
	}

}

/*
 *
 * #include <iostream>
#include <math.h>
using namespace std;


void merge(int numbers[], int low,int middle,int high)
{
	int helper[high]={};
	for(int i=0;i<high;i++)
		helper[i]=numbers[i];
	int i=low;
	int j=middle+1;
	int k=low;
	
	for(;i<=middle && j<high;k++)
	{
		if(helper[i]<=helper[j])
		{
			numbers[k]=helper[i];
			i++;
		}
		else
		{
			numbers[k]=helper[j];
			j++;
		}
	}
	for(;i<=middle;)
	{
		numbers[k]=helper[i];
		k++;
		i++;
	}
}
void mergeSort(int numbers[],int low,int high)
{
	int middle = low + (high-low)/2;
	if(low<high)
	{
		mergeSort(numbers,low,middle);
		mergeSort(numbers,middle+1,high);
		merge(numbers,low,middle,high);
	}
}
int findAbs(int a ,int b)
{
	return a > b ? a-b:b-a;
}
void findMaximumDifference(int numbers[],int low,int high)
{
	if(high==1)
		cout<<numbers[0];
	else if(high==2)
		cout<<findAbs(numbers[1],numbers[0]);
	else
	{
		mergeSort(numbers,low,high);
		cout<<"MaximumDiff"<<(numbers[high-1] - numbers[0]);
	}
}
int main(void) {
	// your code goes here
	int numbers[] ={-1,4};
	mergeSort(numbers,0,2);
	for(int i=0;i<2;i++)
		cout<<numbers[i]<<"\n";
	findMaximumDifference(numbers,0,2);
	return 0;
}

 * 
 */
 
