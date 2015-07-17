Eventsrc
======

[ ![Codeship Status for atlassianlabs/eventsrc](https://codeship.com/projects/444e9790-0e8b-0133-73db-5acaa1e219b5/status?branch=master)](https://codeship.com/projects/91708)

A library for implementing event sourcing data persistence in your Scala project.

*Please note that this code is still in early but active development*

## What is event sourcing

The basic concept is that we represent the state of data as a series of immutable 'events'. These events are ordered,
 and when we replay them in time-sequence order, we can generate a view or snapshot of our data at any point in time.
 This gives us a number of useful characteristics including:
 
 * In-built audit trail and recoverability - since we don't delete or mutate data, we can always determine what our data was at any point in time.
 * Flexibility in our queries - By re-interpreting the events we can generate new views from existing data.
 * Easy support for a messaging-based service architecture - We can also send these events to other systems
     in a reliable and scalable fashion where it can be processed, typically in a more efficient manner for a specific use case e.g. sending events to
     ElasticSearch for full-text search of entities. We can also send these events to a remote replica easily for disaster recovery.

There is a slide deck walking through the EventStream library in the doc directory.

## Usage

### Adding the dependency

Currently the artifacts are published to Atlassian's public Maven repository, so you'll need to add the following resolver:
    
    resolvers += "atlassian-public"   at "https://maven.atlassian.com/content/groups/atlassian-public/"

The project is split into separate modules for each type of AWS API so you can import them separately if you wish (e.g. `aws-scala-s3`, `aws-scala-dynamodb`, `aws-scala-sqs`, `aws-scala-cloudformation`).
Alternatively, you can:

    libraryDependencies += "io.atlassian" %% "eventsrc"  % "0.0.16"
    
### Choosing EventSource or EventStream

We have two separate implementations of event sourcing - one in `io.atlassian.event.source` package and one in `io.atlassian.event.stream` package. 
The `source` implementation was our first version (currently being used in production), but has some limitations:
 
 * Standard event types (`Insert` and `Delete`)
 * No snapshot caching concept
 * Unable to plugin different event storage layers
 * Query only on the event stream key
 
The `stream` implementation is our new version under active development that addresses these issues. Once this is stable we will reimplement the `source` version using `stream`.
It is recommended that new work uses the `stream` version (although be aware interfaces may/likely change).

#### Using EventSource

The best place to see this is in the test (`io.atlassian.event.source.EventSourceSpec` and the memory implementation `io.atlassian.event.source.MemoryEventSource`).
 
There is also a Dynamo implementation that uses the `aws-scala` library in the `dynamodb` module.

#### Using EventStream

At the moment, the best place to start is in the `test` tree in the `core` module. There is `UserAccountExample` as well as a base spec `DirectoryEventStreamSpec` with an implementation
in the `memory` package that uses an in-memory (non-thread-safe) event store.

There is also a Dynamo implementation that uses the `aws-scala` library in the `dynamodb` module, with an accompanying test in the `test` tree.

`EventStream` bakes in the concept of snapshot caching. There is a basic in-memory version of snapshot storage in the `memory` package in `core` module, and a Dynamo version in the `dynamo` module.


## TODO

  * Better documentation and more examples
  * Safer retry in event saving logic (max retries, exponential backoff)
  * Generic Redis-based snapshot storage implementation

## Developing

This project is a pretty standard Scala/SBT project using `specs2`. Have a look at the existing specs for examples. We are using immutable specs.

### Scalariform

For consistent code formatting, we're using Scalariform. There is a Git pre-commit hook under `project/hooks` that you can/should put into
your `.git/hooks` directory that will run Scalariform before all commits.

### local versus integration tests

There are a bunch of local tests for things that can be tested locally, and integration tests for things that need to talk to AWS resources.
DynamoDB specs are a little different in that the spec spins up a [local DynamoDB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
in local mode, and in integration mode it talks with actual DynamoDB

To run the local tests, just do the normal `sbt test`.

To run integration tests, you will need to:

  1. Set up AWS access keys as per standard AWS Java SDK settings (e.g. `AWS_SECRET_KEY` and `AWS_ACCESS_KEY_ID` environment variables)
  2. Ensure that you have `gtimeout` and `wget` installed e.g. `brew install coreutils` and `brew install wget` on Mac OS X
  3. Run the integration tests via `sbt 'test-only -- aws-integration'`
  

### Publishing and releasing

To release and publish, use the standard `sbt`-ism:

    sbt publish             # To publish a snapshot to maven private snapshot repo
    sbt 'release cross'     # To tag a release and publish to maven private release repo
    
Obviously be sure the run the integration before releasing.
