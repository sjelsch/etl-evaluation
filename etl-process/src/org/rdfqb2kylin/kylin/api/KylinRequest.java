package org.rdfqb2kylin.kylin.api;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.rdfqb2kylin.etl.Property;

/*
 * Kylin Request Class
 * Handles JSON Requests to Kylin Server
 */
public class KylinRequest {
	private final static Logger logger = Logger.getLogger(KylinRequest.class);

	public static JSONObject get(String url) {
		try {
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();

			HttpGet request = new HttpGet(url);
			addHeader(request);

			HttpResponse response = httpClient.execute(request);

			int statusCode = response.getStatusLine().getStatusCode();
			if (statusCode != 200) {
				JSONObject responseJSONObject = new JSONObject(EntityUtils.toString(response.getEntity()));
				throw new Exception("Kylin GET Request failed with StatusCode " + statusCode + "! Reason: " + responseJSONObject.get("exception"));
			}

			return new JSONObject(EntityUtils.toString(response.getEntity()));
		} catch (Exception e) {
			logger.error("Kylin GET Request failed!");
			e.printStackTrace();
			System.exit(0);
		}

		return null;
	}

	public static JSONObject put(String url, JSONObject jsonObject) {
		try {
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();

			HttpPut request = new HttpPut(url);
			addHeader(request);
			request.setEntity(new StringEntity(jsonObject.toString()));

			HttpResponse response = httpClient.execute(request);
			int statusCode = response.getStatusLine().getStatusCode();

			if (statusCode != 200) {
				JSONObject responseJSONObject = new JSONObject(EntityUtils.toString(response.getEntity()));
				throw new Exception("Kylin PUT Request failed with StatusCode " + statusCode + "! Reason: " + responseJSONObject.get("exception"));
			}

			return new JSONObject(EntityUtils.toString(response.getEntity()));
		} catch (Exception e) {
			logger.error("Kylin PUT Request failed!");
			e.printStackTrace();
			System.exit(0);
		}

		return null;
	}

	public static JSONObject post(String url) {
		return executePOST(url, null);
	}

	public static JSONObject post(String url, JSONObject jsonObject) {
		return executePOST(url, jsonObject);
	}

	private static JSONObject executePOST(String url, JSONObject jsonObject) {
		try {
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();

			HttpPost request = new HttpPost(url);
			addHeader(request);
			if (jsonObject != null) {
				request.setEntity(new StringEntity(jsonObject.toString()));
			}
			logger.debug("URL: " + url);
			logger.debug("jsonObject: " + jsonObject);
			HttpResponse response = httpClient.execute(request);
			int statusCode = response.getStatusLine().getStatusCode();

			if (statusCode != 200) {
				JSONObject responseJSONObject = new JSONObject(EntityUtils.toString(response.getEntity()));
				throw new Exception("Kylin POST Request failed with StatusCode " + statusCode + "! Reason: " + responseJSONObject.get("exception"));
			}

			return new JSONObject(EntityUtils.toString(response.getEntity()));
		} catch (Exception e) {
			logger.error("Kylin POST Request failed!");
			e.printStackTrace();
			System.exit(0);
		}

		return null;
	}

	private static void addHeader(HttpRequestBase request) {
		request.addHeader("Content-Type", "application/json");
		request.addHeader("Accept", "application/json");
		request.addHeader("Authorization", "Basic " + basicAuth());
	}

	private static String basicAuth() {
		return new Base64().encodeAsString(new String(Property.get("kylin.user") + ":" + Property.get("kylin.password")).getBytes());
	}
}
