package com.denodo.connect.odata4.util;

import java.io.IOException;
import java.io.InputStream;

public class ResourcesUtil {

    /**
     * Obtain a resource by name, throwing an exception if it is not present.
     * @param resourceName the name of the resource to be obtained
     * @return an input stream on the resource (null never returned)
     * @throws IOException if the resource could not be located
     */
    public static InputStream loadResourceAsStream(final String resourceName) throws IOException {
        
        final InputStream input = Versions.class.getClassLoader().getResourceAsStream(resourceName);
        if (input != null) {
            return input;
        }
        throw new IOException("Could not locate resource '" + resourceName + "' in the aplication's class path");
    }
}
