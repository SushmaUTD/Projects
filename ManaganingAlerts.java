import java.util.concurrent.TimeUnit;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.safari.SafariDriver;


public class ManaganingAlerts {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		//Step1:  launch a Empty browser.
				WebDriver driver = new SafariDriver();// We can use firefox or chrome browsers to test this
				//Step2:  maximize the browser.
				driver.manage().window().maximize();
				//Step3:  pass the URL("https://accounts.google.com/").
				driver.get("http://www.irctctourism.com/cgi-bin/rr.dll/irctc/services/rrhome.do#"); 
				//implicitly wait
				driver.manage().timeouts().implicitlyWait(20, TimeUnit.SECONDS);
				//Step4:  get the title of the page and print in the console screen.
				System.out.println("Irctc Page Title is : "+driver.getTitle());
				//click on Serach button
				driver.findElement(By.id("submit_button")).click();
		        Thread.sleep(3000);
				//Alert
				Alert  alt = driver.switchTo().alert();
				//Print Alet Message in Console Screen
				System.err.println("IRCTC Alert Message is : "+alt.getText());
				Thread.sleep(3000);
				//click on Ok Button
				alt.accept();
				Thread.sleep(3000);
				//close the Browser
				driver.close();

	}

}
