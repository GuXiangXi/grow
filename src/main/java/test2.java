import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

/****
 * Java读取XML配置文件
 */
public class test2 {
    static File file = new File("person.xml");//Persons.xml文件绝对路径

    public static void main(String[] args) throws Exception {
        //①获得解析器DocumentBuilder的工厂实例DocumentBuilderFactory  然后拿到DocumentBuilder对象
        DocumentBuilder newDocumentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        //②获取一个与磁盘文件关联的非空Document对象
        Document doc = newDocumentBuilder.parse(file);
        //③通过文档对象获得该文档对象的根节点
        Element root = doc.getDocumentElement();
        //通过根节点获得子节点
        NodeList personList = root.getElementsByTagName("person");
        //这里获取第1个节点
        Node item = personList.item(0);
        System.out.println(item.getTextContent());//获取第一个节点的所有子节点值
        Element element = (Element)item;//这里转换成子类类型   ==》原因：父类没有对应的方法    这里只看类型不看值
        //这里获取第1个节点下 name节点值
        NodeList nameList = element.getElementsByTagName("name");
        System.out.println(nameList.item(0).getTextContent());

        //获取第二个节点
        Node item1 = personList.item(1);
        System.out.println(item1.getTextContent());//获取第二个节点所有子节点值
        Element element1 = (Element) item1;
        //获取第二个节点下age节点值
        NodeList age1List = element1.getElementsByTagName("age");
        System.out.println(age1List.item(0).getTextContent());




    }
}
