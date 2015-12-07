
import java.io.IOException;
import java.net.URLEncoder;

import com.depend.jdq.fastjson.JSON;
import com.depend.jdq.fastjson.JSONException;
import com.depend.jdq.fastjson.JSONObject;
import com.depend.jdq.http.HttpEntity;
import com.depend.jdq.http.HttpResponse;
import com.depend.jdq.http.HttpStatus;
import com.depend.jdq.http.client.ClientProtocolException;
import com.depend.jdq.http.client.HttpClient;
import com.depend.jdq.http.client.methods.HttpGet;
import com.depend.jdq.http.client.methods.HttpRequestBase;
import com.depend.jdq.http.impl.client.DefaultHttpClient;
import com.depend.jdq.http.util.EntityUtils;
import com.jd.bdp.jdq.auth.AuthenticationException;

public class CheckConsumerToken {

	public static final String	JDQ_AUTH_JSON_DATA_KEY			= "obj";
	public static final String	JDQ_AUTH_JSON_DATA_ACCESS_KEY	= "code";
	public static final String	JDQ_AUTH_JSON_DATA_ZK_ROOT_KEY	= "zkRoot";
	public static final String	JDQ_AUTH_JSON_DATA_TOPIC_KEY	= "topic";
	public static final String	JDQ_AUTH_JSON_DATA_BROKERS_KEY	= "brokers";

	public static final int		JDQ_AUTH_ACCESS					= 0;

	public static void main(String[] args) throws Exception {

		if (args.length < 0) {
			throw new Exception("Please Check Param,Param is [AppId,Token]!");
		}

		String appId = "rt.report.com";
		String token = "z1PlhRCcLf6qL+bo8UEIzQ==";
		String producerAuthenticationURL = "http://train.bdp.jd.com/api/q/oauth2/authCustomer_v10.ajax?content="
				+ URLEncoder.encode("{\"appId\":\"" + appId + "\",\"token\":\""
						+ token + "\",\"ip\":\"127.0.0.1\"}", "UTF-8");
		JSONObject authJSON = get(producerAuthenticationURL);
		System.out.println(producerAuthenticationURL);

		int access = authJSON.getInteger(JDQ_AUTH_JSON_DATA_ACCESS_KEY);
		if (access != JDQ_AUTH_ACCESS) {
			System.out.println(authJSON.toString());
			throw new AuthenticationException("鉴权未通过，请检查appId，token是否正确.");
		}

		JSONObject authDataJSON = authJSON.getJSONObject(JDQ_AUTH_JSON_DATA_KEY);

		System.out.println("zookeeper信息：" + authDataJSON.getString(JDQ_AUTH_JSON_DATA_ZK_ROOT_KEY));
		System.out.println("topic信息："	+ authDataJSON.getString(JDQ_AUTH_JSON_DATA_TOPIC_KEY));
		System.out.println("broker信息：" + authDataJSON.getString(JDQ_AUTH_JSON_DATA_BROKERS_KEY));

	}

	public static JSONObject get(String url) {
		HttpClient client = new DefaultHttpClient();
		HttpGet get = null;
		try {
			get = new HttpGet(url);
			return response(client, get);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static JSONObject response(HttpClient client,
			HttpRequestBase httpRequest) throws ClientProtocolException,
			IOException, IllegalStateException, JSONException {
		JSONObject response = null;
		HttpResponse res = client.execute(httpRequest);
		if (res.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
			HttpEntity entity = res.getEntity();
			response = JSON.parseObject(EntityUtils.toString(entity, "UTF-8"));
		} else {
			throw new AuthenticationException(
					"鉴权服务器连接失败，请检查host配置或者网络是否正常。错误码:["
							+ res.getStatusLine().getStatusCode() + "]");
		}
		return response;
	}

}
