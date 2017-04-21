
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.safari.SafariDriver;

public class ManagingWithWindows {

	static WebDriver  driver = null;
   //select Latest Window Method
	public void selectLatestWindow(String passThePageTitle){

		Set<String>  set = driver.getWindowHandles();
		System.out.println("Windows Size is : "+set.size());
		for (String count : set) {
			String expectedTitle =passThePageTitle;
			driver.switchTo().window(count);
			if (driver.getPageSource().trim().contains(expectedTitle)) {
				driver.switchTo().window(count);
				driver.manage().window().maximize();
				break;
			}
		}

	}
	//Close Latest Window Method
	public void closeLatestWindow(String passThePageTitle)throws Exception{

		Set<String>  set = driver.getWindowHandles();
		System.out.println("Windows Size is : "+set.size());
		for (String count : set) {
			String expectedTitle =passThePageTitle;
			driver.switchTo().window(count);
			if (driver.getPageSource().trim().contains(expectedTitle)) {
				driver.switchTo().window(count);
				Thread.sleep(2000);
				driver.close();
				break;
			}
		}
	}

	public static void main(String[] args) throws Exception {
        //object Intilization
		ManagingWithWindows  wh = new ManagingWithWindows();
		
		
		//Step1:  launch a Empty browser.
		driver = new SafariDriver();
		//Step2:  maximize the browser.
		driver.manage().window().maximize();
		//Step3:  pass the URL("https://accounts.google.com/").
		driver.get("http://www.naukri.com/"); 
		//implicitly wait
		driver.manage().timeouts().implicitlyWait(20, TimeUnit.SECONDS);
		//Step4:  get the title of the page and print in the console screen.
		System.out.println("Naukri Page Title is : "+driver.getTitle());
		//click on Philips Link
		driver.findElement(By.partialLinkText("NES")).click();
		//close Amazon Window
		wh.closeLatestWindow("Amazon");
		//close Cognizant Window
		wh.closeLatestWindow("Cognizant");
		//Select Nes Window
		wh.selectLatestWindow("Career in NES ñ Jobs in N");
		//clcik on AboutUs Link in Ness Window
		driver.findElement(By.linkText("About Us")).click();
		//close Nes Window
		wh.closeLatestWindow("Career in NES ñ Jobs in N");
		//Select Parrent Window and perform an Operation
		wh.selectLatestWindow("Jobs - Recruitment - Job Search ");
		//send values in location field
		driver.findElement(By.xpath("//input[@name='ql' and @class='sugInp w135']")).sendKeys("Hyderabad");
		Thread.sleep(4000);
		//close the Browser
		driver.close();

	}

}
