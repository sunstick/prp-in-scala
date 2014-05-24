package kvstore

import scala.annotation.migration
import scala.concurrent.duration.DurationInt

import Arbiter.Join
import Arbiter.JoinedPrimary
import Arbiter.JoinedSecondary
import Arbiter.Replicas
import Persistence.Persist
import Persistence.Persisted
import Replicator.Replicate
import Replicator.Replicated
import Replicator.Snapshot
import Replicator.SnapshotAck
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.actorRef2Scala

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import Arbiter._

  arbiter ! Join

  var kv = Map.empty[String, String]
  var secondaries = Map.empty[ActorRef, ActorRef] // a map from secondary replicas to replicators
  var replicators = Set.empty[ActorRef] // the current set of replicators

  val persistence = context.actorOf(persistenceProps)

  var pendingPersist = Map.empty[Long, (ActorRef, Persist)]
  var pendingReplicate = Map.empty[Long, (ActorRef, Set[ActorRef])]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  context.setReceiveTimeout(100.milliseconds)

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      dealWithUpdatesPrimary(key, Some(value), id)
    case Remove(key, id) =>
      kv -= key
      dealWithUpdatesPrimary(key, None, id)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Persisted(key, id) if pendingPersist contains id =>
      pendingPersist -= id
      tryComplete(id)

    case Replicated(key, id) if pendingReplicate contains id =>
      pendingReplicate += id -> (pendingReplicate(id)._1, pendingReplicate(id)._2 - sender)
      tryComplete(id)

    case Replicas(replicas) =>
      for ((replica, replicator) <- secondaries if !replicas.contains(replica) && replica != self) { // old replica
        for ((id, req) <- pendingReplicate) {

          pendingReplicate += id -> (req._1, req._2 - replicator)
          tryComplete(id)
        }
        context.stop(replicator)
        secondaries -= replica
      }

      for (replica <- replicas if !secondaries.contains(replica) && replica != self) { // new replica
        val replicator = context.actorOf(Replicator.props(replica))
        for ((k, v) <- kv)
          replicator ! Replicate(k, Some(v), nextSeq)
        secondaries += (replica -> replicator)
      }
    case ReceiveTimeout =>
      resendPersistReq()
  }

  private def globalAck(id: Long): Boolean = !pendingPersist.contains(id) && pendingReplicate(id)._2.isEmpty
  private def complete(id: Long): Unit = {
    if (globalAck(id)) {
      pendingReplicate(id)._1 ! OperationAck(id)
    } else {
      pendingReplicate(id)._1 ! OperationFailed(id)
    }

    pendingReplicate -= id
    pendingPersist -= id
  }
  private def tryComplete(id: Long): Unit =
    if (globalAck(id))
      complete(id)

  private def dealWithUpdatesPrimary(key: String, valueOption: Option[String], id: Long, from: ActorRef = sender): Unit = {
    persistence ! (Persist(key, valueOption, id))
    pendingPersist += (id -> (from, Persist(key, valueOption, id)))

    for (replicator <- secondaries.values)
      replicator ! Replicate(key, valueOption, id)

    pendingReplicate += (id -> (from, secondaries.values.toSet))

    context.system.scheduler.scheduleOnce(1.second) {
      if (pendingReplicate.contains(id))
        complete(id)
    }
  }

  ////////////////////////////////////////

  var expected = 0L
  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, Some(value), seq) if seq == expected =>
      kv += (key -> value)
      expected += 1
      dealWithUpdatesSecondary(Persist(key, Some(value), seq))
    case Snapshot(key, None, seq) if seq == expected =>
      kv -= key
      expected += 1
      dealWithUpdatesSecondary(Persist(key, None, seq))
    case Snapshot(key, _, seq) if seq < expected => // old ones, future ones are ignored
      sender ! SnapshotAck(key, seq)
    case Persisted(key, id) if pendingPersist.contains(id) =>
      pendingPersist(id)._1 ! SnapshotAck(key, id)
      pendingPersist -= id
    case ReceiveTimeout =>
      resendPersistReq()
  }

  private def dealWithUpdatesSecondary(request: Persist, from: ActorRef = sender): Unit = {
    persistence ! request
    pendingPersist += (request.id -> (from, request))
  }

  private def resendPersistReq(): Unit =
    for ((id, senderReq) <- pendingPersist) {
      val (_, pReq) = senderReq
      persistence ! pReq
    }
}
