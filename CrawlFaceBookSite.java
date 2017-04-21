import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlButton;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlPasswordInput;
import com.gargoylesoftware.htmlunit.html.HtmlSubmitInput;
import com.gargoylesoftware.htmlunit.html.HtmlTextInput;

/**
 * 
 */

/**
 * @author Sushma
 *
 */
public class CrawlFaceBookSite {

	/**
	 * @param args
	 * @throws IOException
	 * @throws MalformedURLException
	 * @throws FailingHttpStatusCodeException
	 */
	public static void main(String[] args)
			throws FailingHttpStatusCodeException, MalformedURLException,
			IOException {
		WebClient webClient = new WebClient(BrowserVersion.FIREFOX_45);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.waitForBackgroundJavaScript(1500);
		final HtmlPage page = webClient.getPage("https://www.facebook.com/");
		webClient.waitForBackgroundJavaScript(1500);
		System.out.println(page.getTitleText());
		HtmlTextInput login_id = (HtmlTextInput) page.getElementById("email");
		HtmlPasswordInput password = (HtmlPasswordInput) page
				.getElementById("pass");
		login_id.setValueAttribute("sxe160530@utdallas.edu");
		password.setValueAttribute("Sushmasush30_");
		HtmlSubmitInput submit = (HtmlSubmitInput) page
				.getHtmlElementById("u_0_n");
		HtmlPage loginResponse = submit.click();
		webClient.waitForBackgroundJavaScript(1500);
		System.out
				.println("Login Successful: " + loginResponse.getTitleText());
		webClient.waitForBackgroundJavaScript(1500);
		List<HtmlElement> searchPeople = (List<HtmlElement>) loginResponse.getByXPath("/html/body/div[1]/div[1]/div/div[1]/div/div/div/div[1]/div[2]/div/form/div/div/div/div/input[2]");
		HtmlTextInput searchInput = (HtmlTextInput) searchPeople.get(0);
		searchInput.setValueAttribute("Krishna Dubagunta");
		HtmlButton searchButton =  (HtmlButton) loginResponse.getByXPath("/html/body/div[1]/div[1]/div/div[1]/div/div/div/div[1]/div[2]/div/form/button");
		webClient.waitForBackgroundJavaScript(1500);
		HtmlPage friendPage = searchButton.click();
		webClient.waitForBackgroundJavaScript(1500);
		System.out.println("Friends Page: "+friendPage.getTitleText());
		webClient.close();

	}

}
