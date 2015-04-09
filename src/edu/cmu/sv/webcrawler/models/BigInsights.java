package edu.cmu.sv.webcrawler.models;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.util.EntityUtils;
import org.xml.sax.SAXException;

public class BigInsights {
	
	private final String CONSOLE_URL = "https://bi-hadoop-prod-569.services.dal.bluemix.net:8443";
	private final String USERNAME = "biblumix";
	private final String PASSWORD = "gPyKu8~zb05u";
	
	private final String GET_DIRECTORY = CONSOLE_URL + "/data/controller/dfs/output?format=xml";
	private final String DOWNLOAD_FILE = CONSOLE_URL + "/data/controller/dfs/tmp/10-K.htm?download=true";

	private final String LIST_APP = CONSOLE_URL + "/data/controller/catalog/applications";
	private final String DESCRIBE_APP = CONSOLE_URL + "/data/controller/catalog/applications/b15cd590-a39e-457c-bb5e-5c9f6c4cd1ec";
	private final String RUN_APP = CONSOLE_URL + "/data/controller/ApplicationManagement";
	private final String LIST_CONFIG_ASSOC_WITH_APP = CONSOLE_URL + "/data/controller/catalog/applications/b15cd590-a39e-457c-bb5e-5c9f6c4cd1ec/runs";
	private final String LIST_JOBS_ASSOC_WITH_APP = CONSOLE_URL + "/data/controller/catalog/applications/b15cd590-a39e-457c-bb5e-5c9f6c4cd1ec/runs";
	private final String LIST_JOB_STATUS = CONSOLE_URL + "/data/controller/ApplicationManagement?actiontype=list_app_execution_status&oozie_id=";
	private final String CONTENT_OF_FILE_PREFIX = CONSOLE_URL + "/data/controller/dfs/output/";
	private final String CONTENT_OF_FILE_POSTFIX = "?download=false";
	private final String CREATE_FILE = CONSOLE_URL + "/data/controller/dfs/tmp/input.txt";
	private final String APPEND_CONTENT = CONSOLE_URL + "/data/controller/dfs/tmp/input.txt";
	private final String DELETE_INPUT = CONSOLE_URL + "/data/controller/dfs/tmp/input.txt";
	
	public String runApp(String content) throws Exception {
		
		try {
			File temp = File.createTempFile("temp", ".tmp");
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
			bw.write(content);
			bw.close();
			
			String req = "curl -i --user "+USERNAME+":"+PASSWORD+" -X POST -L -b cookie.jar \""+CONSOLE_URL+"/data/controller/dfs/tmp/input.txt?op=CREATE&data=true\" --header \"Content-Type:application/octet-stream\" --header \"Transfer-Encoding:chunked\" -T \"" + temp.getAbsolutePath() + "\"";
			
			Process p = Runtime.getRuntime().exec(new String[]{"bash","-c",req});
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Working Directory = "
				+ System.getProperty("user.dir"));
		sendPost(
				RUN_APP,
				"actiontype=run_application&runconfig=<runconfig><name>KeyWordCounter</name><appid>b15cd590-a39e-457c-bb5e-5c9f6c4cd1ec</appid><properties><property><name>datapath</name><value paramtype='PATH'>/tmp</value></property><property><name>outloc</name><value paramtype='DIRECTORYPATH'>/output</value></property><property><name>outputview</name><value paramtype='LIST'>AQL.Counter</value></property></properties></runconfig>");
		String status;
		do  {
			Thread.sleep(1000);
			status = checkLatestJobStatus();
		} while (!status.equals("SUCCEEDED"));			
		String output = sendGet(CONTENT_OF_FILE_PREFIX + getOutputName() + CONTENT_OF_FILE_POSTFIX);
		System.out.println(output);
		deleteInput(DELETE_INPUT);
		return output;
	}

	private String sendGet(String url) throws Exception {

		HttpURLConnection con = urlMethodSetting(url, "get", "");

		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());
		return response.toString();
	}

	private void sendPost(String url, String content) throws IOException {
		HttpURLConnection con = urlMethodSetting(url, "post", content);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());
	}

	private HttpURLConnection urlMethodSetting(String url,
			String method, String postBody) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// need to authenticate before request, userid and password needed to be
		// encoded using Base64
		byte[] encodedBytes = Base64.encodeBase64((USERNAME + ":" + PASSWORD)
				.getBytes());
		String useridNPassword = new String(encodedBytes);
		String authorizationField = "Basic " + useridNPassword;
		System.out.println(authorizationField);
		con.setRequestProperty("Authorization", authorizationField);

		// optional default is GET
		if (method.equals("get") || method.equals("GET")
				|| method.equals("Get"))
			con.setRequestMethod("GET");
		if (method.equals("post") || method.equals("POST")
				|| method.equals("Post")) {
			con.setRequestMethod("POST");
			byte[] outputInBytes = postBody.getBytes("UTF-8");
			con.setDoOutput(true);
			OutputStream os = con.getOutputStream();
			os.write(outputInBytes);
			os.close();
		}
		if (method.equals("delete") || method.equals("DELETE")
				|| method.equals("Delete")) {
			con.setRequestMethod("DELETE");
		}

		int responseCode = con.getResponseCode();
		if (method.equals("get") || method.equals("GET")
				|| method.equals("Get"))
			System.out.println("\nSending 'GET' request to URL : " + url);
		if (method.equals("post") || method.equals("POST")
				|| method.equals("Post"))
			System.out.println("\nSending 'POST' request to URL : " + url);

		System.out.println("Response Code : " + responseCode);

		return con;
	}

	private String getLatestOozieId(String xml) throws SAXException, IOException, ParserConfigurationException {
		int startingIndex = xml.lastIndexOf("<column>") + 8;
		int endingIndex = startingIndex + 36;
		return xml.substring(startingIndex, endingIndex);
	}
	
	private String getLatestJobStatus(String json) {
		int startingIndex = json.indexOf("\"status\"") + 11;
		int endingIndex = json.indexOf("\"", startingIndex);
		return json.substring(startingIndex, endingIndex);
	}

	private String checkLatestJobStatus() throws Exception {
		String xml = sendGet(LIST_JOBS_ASSOC_WITH_APP);
		String oozieId = getLatestOozieId(xml);
		String json = sendGet(LIST_JOB_STATUS + oozieId);
		return getLatestJobStatus(json);
	};
	
	private String getOutputName() throws Exception {
		String dirInfo = sendGet(GET_DIRECTORY);
		int startingIndex = dirInfo.indexOf("<item type=\"file\">") + 18;
		int endingIndex = dirInfo.indexOf("<", startingIndex);
		return dirInfo.substring(startingIndex, endingIndex);
	}
	
	public void create_input(String input) throws IOException, InterruptedException {
		File temp = File.createTempFile("tempfile", ".tmp");
		String path = temp.getAbsolutePath();
		System.out.println(path);
		BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
		bw.write(input);
		bw.close();
		
				
		temp = new File(path);
		
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(CREATE_FILE);
		post.setHeader("Content-Type", "application/octet-stream");
		MultipartEntity entity = new MultipartEntity();
		entity.addPart("upload", new FileBody(temp));
		post.setEntity(entity);

		HttpResponse response = client.execute(post);
		
		//sendPost(APPEND_CONTENT, input);
	}
	
	private void deleteInput(String url) throws IOException {
		HttpURLConnection con = urlMethodSetting(url, "delete", "");

		BufferedReader in = new BufferedReader(new InputStreamReader(
				con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());
	}
}
