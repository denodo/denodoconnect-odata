#!/bin/bash
# -----------------------------------------------------------------------------
#  Environment variable JAVA_HOME must be set and exported
# -----------------------------------------------------------------------------

# -----------------------------------
#  DENODO_HOME variable must be set!
# -----------------------------------
DENODO_HOME=..

export DENODO_CONF="$DENODO_HOME/conf/vdp"

exec "$DENODO_HOME/bin/webcontainer.sh" stop denodo-odata2-service-5.5

