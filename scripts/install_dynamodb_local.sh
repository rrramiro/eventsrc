#!/bin/bash -e

options=':f'

while getopts $options option
do
    case $option in
        f   )   force=1;;
    esac
done

SCRIPT_DIR=`dirname "$0"`
DYNAMO_DB_HOME="$SCRIPT_DIR/../../dynamodb"

if [[ ! -d "$DYNAMO_DB_HOME" ]] || [[ ! -z "$force" ]]; then
    echo "Installing to $DYNAMO_DB_HOME"
    mkdir -p $DYNAMO_DB_HOME
    wget -O $DYNAMO_DB_HOME/dynamodb_local.tar.gz http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest
    tar zx -C $DYNAMO_DB_HOME -f $DYNAMO_DB_HOME/dynamodb_local.tar.gz
    rm $DYNAMO_DB_HOME/dynamodb_local.tar.gz
fi
