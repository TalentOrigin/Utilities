import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CheckName extends UDF {
	public String evaluate(String str) {
		String checkName;
		if (StringUtils.isBlank(str)) {
			return null;
		} else {
			String strArray[] = str.split(",");
			checkName = strArray[0].split(":")[1];
		}
		return checkName;
	} // evaluate
}
