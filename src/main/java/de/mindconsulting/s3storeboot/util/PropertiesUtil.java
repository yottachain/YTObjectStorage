package de.mindconsulting.s3storeboot.util;

import java.io.*;
import java.util.Properties;

public class PropertiesUtil {
	private String path = "D:/NEW-WORK/application.properties";

	public PropertiesUtil() {

	}
	public PropertiesUtil(String file_path) {
		this.path = file_path;
	}
	public String readProperty(String key) {
		String value = "";
		InputStream is = null;
		try {
			is = new FileInputStream(new File(path));
			Properties p = new Properties();
			p.load(is);
			value = p.getProperty(key);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return value;
	}

	public Properties getProperties() {
		Properties p = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(new File(path));
			p.load(is);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return p;
	}

	public void writeProperty(String key, String value) {
		InputStream is = null;
		OutputStream os = null;
		Properties p = new Properties();
		try {
			is = new FileInputStream(new File(path));
			p.load(is);
			os = new FileOutputStream(new File(path));

			p.setProperty(key, value);
			p.store(os, key);
			os.flush();
			os.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (null != is)
					is.close();
				if (null != os)
					os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {
		PropertiesUtil p = new PropertiesUtil("../bin/application.properties");
		String loggingEnable = p.readProperty("s3server.loggingEnabled");
		System.out.println(loggingEnable);
	}
}
