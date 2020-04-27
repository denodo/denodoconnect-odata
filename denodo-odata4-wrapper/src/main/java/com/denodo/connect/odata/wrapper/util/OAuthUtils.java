package com.denodo.connect.odata.wrapper.util;

import static com.denodo.connect.odata.wrapper.util.Naming.GRANT_TYPE_CLIENT_CREDENTIALS;
import static com.denodo.connect.odata.wrapper.util.Naming.GRANT_TYPE_REFRESH_TOKEN;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_GRANT_TYPE_CLIENT_CREDENTIALS;
import static com.denodo.connect.odata.wrapper.util.Naming.INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.denodo.vdb.engine.customwrapper.CustomWrapperException;
import com.denodo.vdb.engine.customwrapper.input.value.CustomWrapperInputParameterValue;

public class OAuthUtils {

    private static final Logger logger = Logger.getLogger(OAuthUtils.class);

    public static void validateOAuthInputParameters(CustomWrapperInputParameterValue accessToken,
        CustomWrapperInputParameterValue refreshToken, CustomWrapperInputParameterValue tokenEndpointURL,
        CustomWrapperInputParameterValue clientId, CustomWrapperInputParameterValue clientSecret,
        CustomWrapperInputParameterValue grantType)
        throws CustomWrapperException {

        // Mandatory fields
        if ((accessToken == null) || StringUtils.isBlank((String) accessToken.getValue())
            || (tokenEndpointURL == null) || StringUtils.isBlank((String) tokenEndpointURL.getValue())
            || (clientId == null) || StringUtils.isBlank((String)clientId.getValue())
            || (clientSecret == null) || StringUtils.isBlank((String) clientSecret.getValue())) {

            logger.error(
                "For Oauth2 authentication: the access token, the refresh token, client id, client secret and the "
                    + "token endpoint URL are required.");
            throw new CustomWrapperException(
                "For Oauth2 authentication: the access token, the refresh token, client id, client secret and the "
                    + "token endpoint URL are required.");
        }

        // Grant type validations: Refresh token field only set for the Refresh token grant type
        if ((refreshToken != null) && StringUtils.isNotBlank((String) refreshToken.getValue())
            && (grantType != null) && !grantType.getValue().equals(INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN)) {

            logger.error(
                "For Oauth2 authentication: Refresh Token field can only be set when Refresh Token grant type is selected.");
            throw new CustomWrapperException(
                "For Oauth2 authentication: Refresh Token field can only be set when Refresh Token grant type is selected.");
        }

        if (((refreshToken == null) || StringUtils.isBlank((String)refreshToken.getValue()))
            && (grantType != null) && grantType.getValue().equals(INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN)) {

            logger.error(
                "For Oauth2 authentication: Refresh Token field is mandatory when Refresh Token grant type is selected.");
            throw new CustomWrapperException(
                "For Oauth2 authentication: Refresh Token field is mandatory when Refresh Token grant type is selected.");
        }

    }

    public static String getOAuthGrantType(CustomWrapperInputParameterValue value) {

        if (value != null && value.getValue() != null) {

            // OAuth flows
            switch (String.valueOf(value)) {
                case INPUT_PARAMETER_GRANT_TYPE_CLIENT_CREDENTIALS : return GRANT_TYPE_CLIENT_CREDENTIALS;
                case INPUT_PARAMETER_GRANT_TYPE_REFRESH_TOKEN : return GRANT_TYPE_REFRESH_TOKEN;
            }
        }

        // By default, refresh token
        return GRANT_TYPE_REFRESH_TOKEN;
    }

    public static Map<String, String> getOAuthExtraParameters(String input) throws CustomWrapperException {

        // The logic has been already defined for the http headers
        return HttpUtils.getHttpHeaders(input);
    }

    public static String getRefreshTokenFromInput(CustomWrapperInputParameterValue refreshToken) {
        String refreshTokenValue = null;
        if (refreshToken != null && StringUtils.isNotBlank((String) refreshToken.getValue())) {
            refreshTokenValue = (String) refreshToken.getValue();
        }

        return refreshTokenValue;
    }
}

