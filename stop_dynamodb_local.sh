#!/bin/sh -e

SCRIPT_DIR=`dirname "$0"`
PORT=${1:-8000}
DYNAMO_DB_HOME="$SCRIPT_DIR/../../dynamodb/$PORT"

cat $DYNAMO_DB_HOME/dynamodb_local.pid | xargs kill
rm $DYNAMO_DB_HOME/dynamodb_local.pid
