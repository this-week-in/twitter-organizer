package com.joshlong.twitter.organizer;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */

@ConfigurationProperties("twitter.organizer")
public class TwitterOrganizerProperties {

	public String getAccessTokenSecret() {
		return accessTokenSecret;
	}

	public void setAccessTokenSecret(String accessTokenSecret) {
		this.accessTokenSecret = accessTokenSecret;
	}

	public String getAccessToken() {
		return accessToken;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public String getClientKeySecret() {
		return clientKeySecret;
	}

	public void setClientKeySecret(String clientKeySecret) {
		this.clientKeySecret = clientKeySecret;
	}

	public String getClientKey() {
		return clientKey;
	}

	public void setClientKey(String clientKey) {
		this.clientKey = clientKey;
	}

	private String accessTokenSecret;
	private String accessToken;
	private String clientKeySecret;
	private String clientKey;
}
