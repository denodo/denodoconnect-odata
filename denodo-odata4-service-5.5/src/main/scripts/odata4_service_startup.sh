#!/bin/bash
# -----------------------------------------------------------------------------
#  Environment variable JAVA_HOME must be set and exported
# -----------------------------------------------------------------------------

# -----------------------------------
#  DENODO_HOME variable must be set!
# -----------------------------------
DENODO_HOME=..

DENODO_JRE_HOME="$DENODO_HOME/jre"
DENODO_JAVA_HOME="$DENODO_JAVA_HOME"

if [ "$DENODO_JAVA_HOME" != "" ]
then
    if [ -e "$DENODO_JAVA_HOME/jre/bin/java" ]
    then
        JAVA_BIN="$DENODO_JAVA_HOME/jre/bin/java"
    else
        JAVA_BIN="$DENODO_JAVA_HOME/bin/java"
    fi
fi
if [ ! -e "$JAVA_BIN" ]
then
    if [ -d "$DENODO_JRE_HOME" ]
    then
        JAVA_BIN=$DENODO_JRE_HOME/bin/java
    fi
fi
if [ ! -e "$JAVA_BIN" ]
then
    if [ -e "$JAVA_HOME/jre/bin/java" ]
    then
        JAVA_BIN="$JAVA_HOME/jre/bin/java"
    else
        JAVA_BIN="$JAVA_HOME/bin/java"
    fi
fi

DENODO_LAUNCHER_CLASSPATH="$DENODO_HOME/lib/denodo-launcher-util.jar"
if [ "$DENODO_EXTERNAL_CLASSPATH" != "" ]
then
    DENODO_LAUNCHER_CLASSPATH=$DENODO_LAUNCHER_CLASSPATH:$DENODO_EXTERNAL_CLASSPATH
fi

if [ "$DENODO_CONF" == "" ]
then
    DENODO_CONF="$DENODO_HOME/conf/launcher"
fi


if [ -e "$JAVA_BIN" ]
then

           echo "Starting Web Container..."
  	"$JAVA_BIN" \
          -classpath "$DENODO_LAUNCHER_CLASSPATH" \
          -DverboseMode=false \
          com.denodo.util.launcher.Launcher \
          com.denodo.tomcat.TomcatBootstrap \
          --conf "$DENODO_CONF" \
          --conf "$DENODO_HOME/resources/apache-tomcat/conf" \
          --lib "$DENODO_HOME/lib/contrib" \
		  --arg start --arg denodo-odata4-service-5.5
       
 
    exit 0

  
else
    echo "Unable to execute $0: Environment variable JAVA_HOME must be set and exported"
fi
