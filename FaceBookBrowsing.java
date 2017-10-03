import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.safari.SafariDriver;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class FaceBookBrowsing {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		//Step1:  launch a Empty browser.
				WebDriver   driver = new SafariDriver();
				//Step2:  maximize the browser.
				driver.manage().window().maximize();
				//Step3:  pass the URL("https://www.facebook.com/").
				driver.get("https://www.facebook.com/");
				//Step4:  get the title of the page and print in the console screen.
				System.out.println("your FaceBook Page Title is : "+driver.getTitle());
				driver.findElement(By.xpath(".//*[@id='email']")).sendKeys("sxe160530@utdallas.edu");
				driver.findElement(By.xpath(".//*[@id='pass']")).sendKeys("**********");
				Thread.sleep(2000);
				driver.findElement(By.xpath(".//*[@id='pass']")).sendKeys(Keys.ENTER);
				Thread.sleep(3000);
				System.out.println("Your logged in page title: "+driver.getTitle());
				//driver.findElement(By.xpath(".//*[@id='pagelet_welcome_box']/ul/li[1]/div/a")).click();
				//Thread.sleep(2000);
				driver.findElement(By.partialLinkText("Games")).click();
				System.out.println("your FaceBook Page Title is : "+driver.getTitle());
				Thread.sleep(2000);
				//Terms Link - PartialLinkText
				driver.findElement(By.partialLinkText("Ter")).click();
				System.out.println("your FaceBook Page Title is : "+driver.getTitle());
				Thread.sleep(2000);
				driver.close();
	}

}
