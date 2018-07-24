package com.denodo.connect.odata4.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Versions {

    public static final double ARTIFACT_ID;
    
    public static final double MINOR_ARTIFACT_ID_SUPPORT_USER_AGENT = 7.0;
    
    static {
        
        InputStream input = null;
        String artifactId = null;
        
        try {
            
            final String filename = "version.properties";
            input = ResourcesUtil.loadResourceAsStream(filename);            

            final Properties properties = new Properties();
            properties.load(input);
            
            artifactId = properties.getProperty("artifactId");
            
            int separatorIdx = artifactId.lastIndexOf('-');
            ARTIFACT_ID = Double.parseDouble(artifactId.substring(separatorIdx + 1));           
            
        } catch (final Exception e) {
            
            throw new ExceptionInInitializerError("Exception during initialization of service versioning utilities");
            
        } finally {
            
            try {
                
                input.close();
                
            } catch (IOException ignored) {

            }
        }
    }
}
