#!/bin/sh -e

options=':fd'

while getopts $options option
do
    case $option in
        f   )   force=1;;
        d   )   dynalite=1;;
    esac
done

# find the timeout command ('timeout' on ubuntu, 'gtimeout' on MacOS X)
for cmd in gtimeout timeout; do
  if type ${cmd} > /dev/null 2>&1; then
    TIMEOUT_CMD=${cmd}
    break
  fi
done
if [ -z "${TIMEOUT_CMD}" ]; then
  echo "It seems you don't have the timeout binary. If you're on MacOS X, try 'brew install coreutils'.";
  exit 1
fi

JAR_HREF=http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest
SCRIPT_DIR=`dirname "$0"`
DYNAMO_DB_LIB_HOME="$SCRIPT_DIR/../../dynamodb"
TIMEOUT_SECONDS=20

if [ ! -z "$dynalite" ]; then
    expectedBinary=$DYNAMO_DB_LIB_HOME/node_modules/.bin/dynalite
else
    expectedBinary=$DYNAMO_DB_LIB_HOME/DynamoDBLocal.jar
fi


mkdir -p "$DYNAMO_DB_LIB_HOME"; WAS_CREATED=$?
if [ ! -e $expectedBinary -o -n "$force" ]; then
    if [ ! -z "$dynalite" ]; then
        # create the node_modules subdir and call npm to install
        echo "Installing Dynalite"
        mkdir -p "$DYNAMO_DB_LIB_HOME/node_modules"
        npm install --prefix $DYNAMO_DB_LIB_HOME dynalite
    else
        # d/l the JAR with curl
        echo "Installing AWS Local DynamoDB"
        mkdir -p "$DYNAMO_DB_LIB_HOME"
        curl -sL "$JAR_HREF" | tar zx -C "$DYNAMO_DB_LIB_HOME"
    fi
fi

if [ \! -e $expectedBinary ]; then
    # do timeout stuff
    echo "Waiting for DynamoDB to become ready"
    ${TIMEOUT_CMD} --foreground ${TIMEOUT_SECONDS} bash -c "until test -e ${expectedBinary}; do sleep 2; done; exit 0"
fi
