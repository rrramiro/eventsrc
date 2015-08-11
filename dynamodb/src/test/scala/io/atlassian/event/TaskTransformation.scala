package io.atlassian.event

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import io.atlassian.aws.WrappedInvalidException
import io.atlassian.aws.dynamodb._
import DynamoDBAction._

import scalaz.~>
import scalaz.concurrent.Task

object TaskTransformation {
  def runner(client: AmazonDynamoDB): DynamoDBAction ~> Task =
    new (DynamoDBAction ~> Task) {
      def apply[A](a: DynamoDBAction[A]): Task[A] =
        a.runAction(client).fold({ i => Task.fail(WrappedInvalidException.orUnderlying(i)) }, { a => Task.now(a) })

    }
}
