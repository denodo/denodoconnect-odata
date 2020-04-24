package com.denodo.connect.odata.wrapper.util;

import static com.denodo.connect.odata.wrapper.util.Naming.FILE_VERSIONS;

import java.io.InputStream;
import java.util.Properties;

public final class Versions {

	public static final double ARTIFACT_ID;

	public static final double MINOR_ARTIFACT_ID_SUPPORT_DIFFERENT_FORMAT_DATES = 7.0;

	static {

		String artifactId = null;

		try {

			final InputStream input = ResourcesUtil.loadResourceAsStream(FILE_VERSIONS);

			final Properties properties = new Properties();
			properties.load(input);

			artifactId = properties.getProperty("artifactId");

		} catch (final Exception ignored) {

		}

		try {

			int separatorIdx = artifactId.lastIndexOf('-');
			ARTIFACT_ID = Double.parseDouble(artifactId.substring(separatorIdx + 1));

		} catch (final Exception e) {

			throw new ExceptionInInitializerError("Exception during initialization of custom wrapper versioning utilities");
		}

	}
}
