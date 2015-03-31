package io.atlassian.event

import scalaz._
import scalaz.syntax.validation._

object DataValidator {
  type Valid = Boolean
  type ValidationResult = ValidationNel[Reason, Valid]
  type Validator[A] = A => ValidationResult
  val success = true.successNel[Reason]

  implicit class stringValidatorSyntax(val s: String) extends AnyVal {
    def fail: ValidationResult =
      Reason(s).failureNel[Valid]
  }

  def check(f: => Boolean, reason: String): ValidationResult =
    if (f) success else reason.fail
}

