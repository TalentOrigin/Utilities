import org.apache.commons.lang.StringUtils;

public class CheckStatus {
	public String evaluate(String str) {
		String checkName;
		if (StringUtils.isBlank(str)) {
			return null;
		} else {
			String strArray[] = str.split(",");
			checkName = strArray[1].split(":")[1];
		}
		return checkName;
	} // evaluate
}
