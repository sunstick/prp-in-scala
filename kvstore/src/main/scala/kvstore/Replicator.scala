package kvstore

import scala.concurrent.duration.DurationInt

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.actorRef2Scala

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher

  context.setReceiveTimeout(100.milliseconds)

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case rep @ Replicate(key, valueOption, id) =>
      val seq = nextSeq
      replica ! Snapshot(key, valueOption, seq)
      acks += (seq -> (sender, rep))

    case SnapshotAck(key, seq) if acks.contains(seq) =>
      acks(seq)._1 ! Replicated(acks(seq)._2.key, acks(seq)._2.id)
      acks -= seq

    case ReceiveTimeout =>
      for ((seq, senderRep) <- acks) {
        val (_, rep) = senderRep
        replica ! Snapshot(rep.key, rep.valueOption, seq)
      }
  }

}
