#!/bin/bash
source zulip-api-py3-venv/bin/activate

STREAM=$1

case "$STREAM" in
    random|development)
        python zulip/integrations/bridge_with_matrix/matrix_bridge.py -c bridge_${STREAM}.conf 
#--show-join-leave
        ;;
    *)
        echo "ERROR: unknown or empty stream: >$STREAM<"; exit 4 ;;
esac

