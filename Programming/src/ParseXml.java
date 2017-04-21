import java.io.File;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author Sushma
 *
 */
public class ParseXml {
	public static List<String> tags = new ArrayList<String>();

	public static void main(String[] args) {
		try {
			// String xmlFile = args[0];
			tags.add("<glossary>-<title>");
			tags.add("<GlossDiv>-<title>");
			tags.add("<GlossList>");
			tags.add("<GlossEntry>-<attr:ID>-<attr:SortAs>-<GlossTerm>-<Acronym>-<Abbrev>");
			tags.add("<GlossDef>-<para>");
			tags.add("<GlossSeeAlso>-<attr:OtherTerm>");
			tags.add("</GlossSeeAlso>");
			tags.add("</GlossDef>");
			tags.add("<GlossSee>-<attr:OtherTerm>");
			tags.add("</GlossSee>");
			tags.add("</GlossEntry>");
			tags.add("</GlossList>");
			tags.add("</GlossDiv>");
			tags.add("</glossary>");
			File xmlData = new File(
					"/Users/Sushma/Desktop/WPL_Asgnmt_4/sample.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xmlData);
			doc.getDocumentElement().normalize();
			StringBuilder jsonString = new StringBuilder("{\n");
			for (int k = 0; k < tags.size(); k++) {
				String[] tag_names = tags.get(k).split("-");
				for (int j = 0; j < tag_names.length; j++) {
					if (tag_names[j].contains("</")) {
						if (j != tag_names.length - 1)
						{
							
							jsonString.append("\n},");
						}
						else
							jsonString.append("\n}");
						continue;
					}
					if (j == 0) {
						int length = tag_names.length;
						String name = tag_names[j]
								.substring(1, tag_names[j].length() - 1);
						jsonString.append('"'+name+'"'+":{\n");
						NodeList nodes = doc.getElementsByTagName(name);
						for (int i = 0; i < nodes.getLength(); i++) {
							Node node = nodes.item(i);
							for (int m = 1; m < length; m++,j++) {	
								
								if (node.getNodeType() == Node.ELEMENT_NODE) {
									Element element = (Element) node;
									String tagName = "";
									if(tag_names[m].contains("attr:"))
									{
									tagName=tag_names[m].substring(6, tag_names[m].length()-1);
									if(!tagName.contains("OtherTerm"))
										jsonString.append('"'+tagName+'"'+": "+'"'+element.getAttribute(tagName)+'"'+",\n");
									else if(tagName.contains("OtherTerm")&& !(jsonString.toString().contains("OtherTerm")))
										jsonString.append('"'+tagName+'"'+": "+'['+'{'+'"'+element.getAttribute(tagName)+'"'+'}'+",\n");
									else if(tagName.contains("OtherTerm")&& (jsonString.toString().contains("GlossSee")))
										jsonString.append('"'+tagName+'"'+": "+'['+'{'+'"'+element.getAttribute(tagName)+'"'+'}'+",\n");
									else
										jsonString.append('"'+element.getAttribute(tagName)+'"'+'}'+"]"+"\n");
										
									}
									
									
									else
									{
										tagName=tag_names[m]
												.substring(1, tag_names[m].length() - 1);
										jsonString.append('"'+tagName+'"'+": "+'"'+getValue(tagName,
												element)+'"'+",\n");
									}
							
									
									
								}
							}
						}
					}
					
				}
			}
			System.out.println(jsonString+"\n}");
		} catch (Exception e) {
			System.out.println("Exception while reading file" + e);
		}
	}

	private static String getValue(String tag, Element element) {
		NodeList nodes = element.getElementsByTagName(tag).item(0)
				.getChildNodes();
		Node node = (Node) nodes.item(0);
		return node.getNodeValue();
	}
}
