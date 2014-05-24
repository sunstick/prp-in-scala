/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
    case req: Operation => root ! req

  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case req: Operation => pendingQueue = pendingQueue.enqueue(req)
    case CopyFinished => {
      root = newRoot
      context.unbecome()
      while(!pendingQueue.isEmpty) {
        root ! pendingQueue.dequeue._1
        pendingQueue = pendingQueue.dequeue._2
      }
    }
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def left(value: Int): Unit = {
    val l = context.actorOf(props(value, initiallyRemoved = true))
    subtrees = subtrees + (Left -> l)
  }

  def right(value: Int): Unit = {
    val r = context.actorOf(props(value, initiallyRemoved = true))
    subtrees = subtrees + (Right -> r)
  }

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case req@Insert(requester, id, target) =>
      if (target == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (target < elem) {
        if (!subtrees.contains(Left)) left(target)
        subtrees(Left) ! req
      } else {
        if (!subtrees.contains(Right)) right(target)
        subtrees(Right) ! req
      }
    case req@Contains(requester, id, target) =>
      if (target == elem) {
        if (removed) requester ! ContainsResult(id, false)
        else requester ! ContainsResult(id, true)
      } else if (target < elem) {
        if (!subtrees.contains(Left)) requester ! ContainsResult(id, false)
        else subtrees(Left) ! req
      } else {
        if (!subtrees.contains(Right)) requester ! ContainsResult(id, false)
        else subtrees(Right) ! req
      }
    case req@Remove(requester, id, target) =>
      if (target == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else if(target < elem) {
        if (!subtrees.contains(Left)) requester ! OperationFinished(id)
        else subtrees(Left) ! req
      } else {
        if (!subtrees.contains(Right)) requester ! OperationFinished(id)
        else subtrees(Right) ! req
      }

    case CopyTo(treeNode) => {
      var waited = 0

      if (!removed) {
        treeNode ! Insert(self, elem, elem)
        waited += 1
      }
      if (subtrees.contains(Left)) {
        subtrees(Left) ! CopyTo(treeNode)
        waited += 1
      }
      if (subtrees.contains(Right)) {
        subtrees(Right) ! CopyTo(treeNode)
        waited += 1
      }

      if(waited == 0) {
        context.parent ! CopyFinished
        context.stop(self)
      }
      else
        context.become(copying(waited))
    }

    case _ => ???
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(waited: Int): Receive = {
    case OperationFinished(`elem`) =>
      if(waited <= 1) {
        context.parent ! CopyFinished
        context.stop(self)
      } else context.become(copying(waited - 1))
    case CopyFinished =>
      if(waited <= 1) {
        context.parent ! CopyFinished
        context.stop(self)
      } else context.become(copying(waited - 1))
  }

}
