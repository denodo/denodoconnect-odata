package com.denodo.connect.odata4.util;

public class QueryUtil {

    /**
     * Logger for this class
     */
    /*
    private static final Logger logger = LogManager.getLogger(QueryUtil.class);

    public static final String INTEGER = "int";
    public static final String DOUBLE = "double";
    public static final String DECIMAL = "decimal";
    public static final String FLOAT = "float";
    public static final String LONG = "long";
    public static final String MONEY = "money";
    public static final String LOCALDATE = "localdate";
    public static final String STRING = "string";
    public static final String BOOLEAN = "boolean";

    public static final String WHERE_EXPRESSION = "WHEREEXPRESSION";
    public static final String CONTEXT_EXPRESSION = "CONTEXTEXPRESSION";

    public static final int SELECT_TYPE = 1;
    public static final int INSERT_TYPE = 2;
    public static final int DELETE_TYPE = 3;
    public static final int UPDATE_TYPE = 4;
    public static final int LIST_TYPE = 5;
    public static final int DESC_TYPE = 6;

    public static final int UPPER_CASE_POLICY = 0;
    public static final int LOWER_CASE_POLICY = 1;
    public static final int MIXED_CASE_POLICY = 2;

    public static final long UNLIMITED_TUPLES = -1;

    public static final boolean DISPLAY_VERBOSE_ERRORS;

    private static final String DSNAME = "vdpWSDataSource";

    private static final String URI_PARAMETER = "vdbUri";

    public static final String LOGIN_PARAMETER = "username";

    public static final String PASSWORD_PARAMETER = "password";

    private static final String PASSWORDENCRYPTED_PARAMETER = "isPassEncrypted";

    private static final String CHUNKSIZE_PARAMETER = "chunkSize";

    private static final String CHUNKTIMEOUT_PARAMETER = "chunkTimeout";

    private static final String QUERYTIMEOUT_PARAMETER = "queryTimeout";

    private static final String POOLENABLED_PARAMETER = "poolEnabled";

    private static final String POOLINITSIZE_PARAMETER = "poolInitSize";

    private static final String POOLMAXACTIVE_PARAMETER = "poolMaxActive";

    private static final String WEBSERVICENAME_PARAMETER = "webAppName";

    private static final String DISPLAY_VERBOSE_ERRORS_PARAMETER = "displayVerboseErrorMessages";

    private static final String REST_AUTHTYPE_PARAMETER = "httpAuthType";
    private static final String SOAP_AUTHTYPE_PARAMETER = "soapAuthType";

    private static String _uri = null;

    private static String login = null;

    private static String password = null;

    private static boolean passwordEncrypted = false;

    private static long chunkTimeout = 0;

    private static long queryTimeout = 0;

    private static long chunkSize = 0;

    private static boolean poolEnabled = false;

    private static Integer poolInitSize = null;

    private static Integer poolMaxActive = null;

    private static String webServiceName = null;
    */
    
    /*
    static {
        boolean isGlobalRESTfulService;
        try {
            
            Boolean serviceMode = ConfigurationParametersManager.getBooleanParameter("ServiceMode");
            isGlobalRESTfulService = !(serviceMode).booleanValue();

        } catch (MissingConfigurationParameterException e1) {

            logger.debug("This is not a RESTful Web service.");
            isGlobalRESTfulService = false;
        }

        try {
        webServiceName = ConfigurationParametersManager
                .getParameter(WEBSERVICENAME_PARAMETER);
        } catch (MissingConfigurationParameterException e) {
            logger.debug("Configuration parameter '" + WEBSERVICENAME_PARAMETER + "' is missing.");
        }

        try {

            _uri = ConfigurationParametersManager.getParameter(URI_PARAMETER);
            if (logger.isDebugEnabled())
                logger.debug("Obtained uri parameter value " + _uri);
            chunkSize = ConfigurationParametersManager
                    .getLongParameter(CHUNKSIZE_PARAMETER);
            chunkTimeout = ConfigurationParametersManager
                    .getLongParameter(CHUNKTIMEOUT_PARAMETER);
            queryTimeout = ConfigurationParametersManager
                    .getLongParameter(QUERYTIMEOUT_PARAMETER);
            if (!isGlobalRESTfulService) {

                login = ConfigurationParametersManager.getParameter(LOGIN_PARAMETER);
                if (logger.isDebugEnabled())
                    logger.debug("Obtained login parameter value " + login);
                password = ConfigurationParametersManager.getParameter(PASSWORD_PARAMETER);
                logger.debug("Obtained password parameter value.");
            }
        } catch (MissingConfigurationParameterException e) {

            logger.fatal("Some configuration parameters are missing.", e);
        }

        if (!isGlobalRESTfulService) {

            try {

                passwordEncrypted = Boolean.parseBoolean(
                        ConfigurationParametersManager.getParameter(PASSWORDENCRYPTED_PARAMETER));
                if (logger.isDebugEnabled())
                    logger.debug("Obtained is password encrypted parameter value " + passwordEncrypted);

            } catch (MissingConfigurationParameterException e) {
                logger.warn("Missing is password encrypted configuration parameters");
            }

            if (passwordEncrypted){
                password = WsUtil.clearString(password);
            }
        }
        try {

            String restAuthypeParameter = ConfigurationParametersManager.getOptionalParameter(REST_AUTHTYPE_PARAMETER);
            String soapAuthypeParameter = ConfigurationParametersManager.getOptionalParameter(SOAP_AUTHTYPE_PARAMETER);

            if ((restAuthypeParameter != null)
                    && ((AuthenticationType.valueOf(restAuthypeParameter) == AuthenticationType.AUTH_HTTP_SPNEGO)
                    || (AuthenticationType.valueOf(restAuthypeParameter) == AuthenticationType.AUTH_HTTP_SAML2)
                    || (AuthenticationType.valueOf(restAuthypeParameter) == AuthenticationType.AUTH_HTTP_OAUTH2))){
                poolEnabled = false;
            } else if ((soapAuthypeParameter != null)
                    && (AuthenticationType.valueOf(soapAuthypeParameter) == AuthenticationType.AUTH_HTTP_SPNEGO
                    || AuthenticationType.valueOf(restAuthypeParameter) == AuthenticationType.AUTH_HTTP_OAUTH2)) {
                poolEnabled = false;
            } else {
                poolEnabled = ConfigurationParametersManager.getBooleanParameter(POOLENABLED_PARAMETER);
                poolInitSize = ConfigurationParametersManager.getIntParameter(POOLINITSIZE_PARAMETER);
                logger.debug("Obtained pool initial size parameter value " + poolInitSize);

                poolMaxActive = ConfigurationParametersManager.getIntParameter(POOLMAXACTIVE_PARAMETER);
                logger.debug("Obtained pool max active parameter value " + poolMaxActive);
            }

        } catch (MissingConfigurationParameterException e) {
            logger.warn("missing configuration parameters", e);
        }

        logger.debug("Is pool enabled: " + poolEnabled);

        boolean displayVerboseErrors;
        if (isGlobalRESTfulService) {

            displayVerboseErrors = true;

        } else {
            try {
                displayVerboseErrors = ConfigurationParametersManager
                        .getBooleanParameter(DISPLAY_VERBOSE_ERRORS_PARAMETER);

            } catch (MissingConfigurationParameterException e) {
                displayVerboseErrors = true;
                logger.warn("Missing configuration parameter: " + DISPLAY_VERBOSE_ERRORS_PARAMETER);
            }
        }
        DISPLAY_VERBOSE_ERRORS = displayVerboseErrors;
    }
    */
    
    /*
    public static VDBDataSource createDataSource(Properties properties) throws DataSourceException {
        
        long chunkSizeValue = 100;
        long chunkTimeoutValue = 90000;
        long queryTimeoutValue = 80000;
        try {
            chunkSizeValue = chunkSize;
            chunkTimeoutValue = chunkTimeout;
            queryTimeoutValue = queryTimeout;
        } catch (Exception e) {
            logger.fatal("Unable to configure data source");
        }
        VDBDataSource dataSource = null;
        PoolConfig poolConfig = null;
        if (poolEnabled) {
            poolConfig = new PoolConfig();
            if (poolInitSize != null) {
                poolConfig.setInitialSize(poolInitSize);
            }
            if (poolMaxActive != null) {
                poolConfig.setMaxActive(poolMaxActive);
            }
        }

        if (StringUtils.isEmpty(properties.getProperty(ConnectionConstants.PARAM_SERVER_URL)) && _uri != null) {
            properties.setProperty(ConnectionConstants.PARAM_SERVER_URL, _uri);
        }
        properties.setProperty(ConnectionConstants.PARAM_QUERY_TIMEOUT, String.valueOf(queryTimeoutValue));
        properties.setProperty(ConnectionConstants.PARAM_CHUNK_TIMEOUT, String.valueOf(chunkTimeoutValue));
        properties.setProperty(ConnectionConstants.PARAM_CHUNK_SIZE, String.valueOf(chunkSizeValue));
        properties.setProperty(ConnectionConstants.PARAM_POOL_ENABLED, String.valueOf(poolEnabled));
        if (webServiceName != null) {
            properties.setProperty(ConnectionConstants.PARAM_WEB_SERVICE_NAME, webServiceName);
        }
        dataSource = new VDBPoolableDataSource(properties, poolConfig);
        if (logger.isDebugEnabled()) {
            StringBuilder logMessage = new StringBuilder("Created datasource ");
            logMessage.append((poolEnabled) ? "with custom pool config" : "without pool");
            logger.debug(logMessage);
        }
        return dataSource;
    }
    */
}
