#!/bin/sh -e

while getopts ':dp:t:' option
do
    case $option in
        d   )   dynalite=1;;
        p   )   portOpt=$OPTARG;;
        t   )   timeoutOpt=$OPTARG;;
    esac
done

SCRIPT_DIR=`dirname "$0"`
PORT=${portOpt:-8000}
DYNAMO_DB_LIB_HOME="$SCRIPT_DIR/../../dynamodb"
DYNAMO_DB_HOME="$DYNAMO_DB_LIB_HOME/$PORT"
TIMEOUT_SECONDS=${timeoutOpt:-30}

mkdir -p $DYNAMO_DB_HOME/$PORT

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

echo "Starting Local DynamoDB..."

if [ ! -z "$dynalite" ]; then
    eval "($DYNAMO_DB_LIB_HOME/node_modules/.bin/dynalite --port $PORT) &"
else
    eval "(java -Djava.library.path=$DYNAMO_DB_LIB_HOME/DynamoDBLocal_lib -jar $DYNAMO_DB_LIB_HOME/DynamoDBLocal.jar --dbPath $DYNAMO_DB_HOME --port $PORT) &"
fi

SUCCESS=$?

if [ "$SUCCESS" -eq 0 ]; then
    echo $! > $DYNAMO_DB_HOME/dynamodb_local.pid
fi

${TIMEOUT_CMD} --foreground ${TIMEOUT_SECONDS} bash -c "until netstat -nlt | grep \":${PORT}\\b\" > /dev/null; do echo -n .; sleep 2; done; exit 0"

# For some reason we need this echo for Scala Process to return correctly when spinning up a DynamoDB within DynamoDB Specs"
echo "Started Local DynamoDB..."
exit $SUCCESS
