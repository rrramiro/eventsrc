package io.atlassian.event
package source

import org.specs2.Specification

object TransformOpSpec extends Specification {
  def is = s2"""
  Transform.Op
  
    is insert $isInsert
    is delete $isDelete
  """

  def isInsert = "insert" match { case Transform.Op(op) => op === Transform.Op.Insert }

  def isDelete = "delete" match { case Transform.Op(op) => op === Transform.Op.Delete }
}