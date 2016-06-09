#!/bin/bash
# -----------------------------------------------------------------------------
#  Environment variable JAVA_HOME must be set and exported
# -----------------------------------------------------------------------------

# -----------------------------------
#  DENODO_HOME variable must be set!
# -----------------------------------
DENODO_HOME=..

export DENODO_CONF="$DENODO_HOME/conf/vdp"

exec "$DENODO_HOME/bin/webcontainer.sh" stop denodo-odata4-service-6.0

