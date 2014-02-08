package org.zeromq.examples;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

public class Examples {
	private static Random rand = new Random(System.currentTimeMillis());
	
	public static String getProperty(String key, String defaultValue) {
		return System.getProperty(key, defaultValue);
	}
	
	public static int getProperty(String key, int defaultValue) {
		int intValue = defaultValue;
		try {
			String value = System.getProperty(key);
			if (value != null) {
				intValue = Integer.parseInt(value);
			}
		} catch (NumberFormatException ignored) {
		}
		
		return intValue;
	}
	
	public static long getProperty(String key, long defaultValue) {
		long longValue = defaultValue;
		try {
			String value = System.getProperty(key);
			if (value != null) {
				longValue = Long.parseLong(value);
			}
		} catch (NumberFormatException ignored) {
		}
		
		return longValue;
	}
	
	public static boolean getProperty(String key, boolean defaultValue) {
		char firstCharacter;
		boolean boolValue = defaultValue;
		String value = System.getProperty(key);
		if (value != null && value.length() > 0) {
			firstCharacter = Character.toLowerCase(value.charAt(0));
			switch (firstCharacter) {
				case 't':
				case 'y':
				case '1':
				case '+':
					boolValue = true;
					break;
				case 'f':
				case 'n':
				case '0':
				case '-':
					boolValue = false;
					break;
			}
		}
		
		return boolValue;
	}
	
	public static String getRandomIdentity() {
		String host;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException ex) {
			throw new Error(ex);
		}
		
		return String.format("%s-%04X", host, String.valueOf(rand.nextInt()));
	}
}
