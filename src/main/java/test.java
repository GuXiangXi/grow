

public class test {
    public static void main(String[] args) {

        String isNoticeUser = ReadXMLConfig.getInstance().getConfigValue(
                "isNoticeUser", "123");
        String userName = ReadXMLConfig.getInstance().getConfigValue(
                "userName", "123");
        String userPwd = ReadXMLConfig.getInstance().getConfigValue("userPwd",
                "123");
        String userAge = ReadXMLConfig.getInstance().getConfigValue("userAge",
                "123");
        String userSex = ReadXMLConfig.getInstance().getConfigValue("userSex",
                "123");
        String emailAddress = ReadXMLConfig.getInstance().getConfigValue("emailAddress",
                "123456@sina.com");

        System.out.println("isNoticeUser=" + isNoticeUser + ";userName="
                + userName + ";userPwd=" + userPwd + ";userAge=" + userAge
                + ";userSex=" + userSex + ";emailAddress=" + emailAddress);
    }
}
