/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.NotUsed
import akka.actor.Status
import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.annotation.InternalApi
import akka.kafka.Subscriptions.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.internal.KafkaConsumerActor.Internal.RegisterSubStage
import akka.kafka.{AutoSubscription, ConsumerFailed, ConsumerSettings}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.pattern.{ask, AskTimeoutException}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{StageLogging, _}
import akka.stream.{ActorMaterializerHelper, Attributes, Outlet, SourceShape}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Internal API.
 *
 * A `SubSourceLogic` is used to create partitioned sources from the various types of sub sources available:
 * [[CommittableSubSource]], [[PlainSubSource]] and [[TransactionalSubSource]].
 *
 * A `SubSourceLogic` emits a source of `SubSourceStage` per subscribed Kafka partition.
 *
 * The `SubSourceLogic.subSourceStageLogicFactory` parameter is passed to each `SubSourceStage` so that a new
 * `SubSourceStageLogic` can be created for each stage. Context parameters from the `SubSourceLogic` are passed down to
 * `SubSourceStage` and on to the `SubSourceStageLogicFactory` when the stage creates a `GraphStageLogic`.
 *
 */
@InternalApi
private class SubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription,
    getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None,
    onRevoke: Set[TopicPartition] => Unit = _ => (),
    subSourceStageLogicFactory: SubSourceStageLogicFactory[K, V, Msg]
) extends TimerGraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with StageLogging {
  import SubSourceLogic._

  val consumerPromise = Promise[ActorRef]
  final val actorNumber = KafkaConsumerActor.Internal.nextNumber()
  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = consumerPromise.future
  var consumerActor: ActorRef = _
  var sourceActor: StageActor = _

  /** Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet. */
  var pendingPartitions: immutable.Set[TopicPartition] = immutable.Set.empty

  /** We have created a source for these partitions, but it has not started up and is not in subSources yet. */
  var partitionsInStartup: immutable.Set[TopicPartition] = immutable.Set.empty
  var subSources: Map[TopicPartition, ControlAndStageActor] = immutable.Map.empty

  /** Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking. */
  var partitionsToRevoke: Set[TopicPartition] = Set.empty

  override def preStart(): Unit = {
    super.preStart()

    sourceActor = getStageActor {
      case (_, Status.Failure(e)) =>
        failStage(e)
      case (_, Terminated(ref)) if ref == consumerActor =>
        failStage(new ConsumerFailed)
    }
    consumerActor = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(sourceActor.ref, settings),
                                        s"kafka-consumer-$actorNumber")
    }
    consumerPromise.success(consumerActor)
    sourceActor.watch(consumerActor)

    def assignmentHandler =
      PartitionAssignmentHelpers.chain(
        new PartitionAssignmentHelpers.AsyncCallbacks(subscription,
                                                      sourceActor.ref,
                                                      partitionAssignedCB,
                                                      partitionRevokedCB),
        subscription.partitionAssignmentHandler
      )

    subscription match {
      case TopicSubscription(topics, _, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal
                             .Subscribe(topics, assignmentHandler),
                           sourceActor.ref)
      case TopicSubscriptionPattern(topics, _, _) =>
        consumerActor.tell(
          KafkaConsumerActor.Internal
            .SubscribePattern(topics, assignmentHandler),
          sourceActor.ref
        )
    }
  }

  private val updatePendingPartitionsAndEmitSubSourcesCb =
    getAsyncCallback[Set[TopicPartition]](updatePendingPartitionsAndEmitSubSources)

  private val stageFailCB = getAsyncCallback[ConsumerFailed] { ex =>
    failStage(ex)
  }

  val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { assigned =>
    val formerlyUnknown = assigned -- partitionsToRevoke

    if (log.isDebugEnabled && formerlyUnknown.nonEmpty) {
      log.debug("#{} Assigning new partitions: {}", actorNumber, formerlyUnknown.mkString(", "))
    }

    // make sure re-assigned partitions don't get closed on CloseRevokedPartitions timer
    partitionsToRevoke = partitionsToRevoke -- assigned

    getOffsetsOnAssign match {
      case None =>
        updatePendingPartitionsAndEmitSubSources(formerlyUnknown)

      case Some(getOffsetsFromExternal) =>
        implicit val ec: ExecutionContext = materializer.executionContext
        getOffsetsFromExternal(assigned)
          .onComplete {
            case Failure(ex) =>
              stageFailCB.invoke(
                new ConsumerFailed(
                  s"#$actorNumber Failed to fetch offset for partitions: ${formerlyUnknown.mkString(", ")}.",
                  ex
                )
              )
            case Success(offsets) =>
              seekAndEmitSubSources(formerlyUnknown, offsets)
          }
    }
  }

  private def seekAndEmitSubSources(
      formerlyUnknown: Set[TopicPartition],
      offsets: Map[TopicPartition, Long]
  ): Unit = {
    implicit val ec: ExecutionContext = materializer.executionContext
    consumerActor
      .ask(KafkaConsumerActor.Internal.Seek(offsets))(Timeout(10.seconds), sourceActor.ref)
      .map(_ => updatePendingPartitionsAndEmitSubSourcesCb.invoke(formerlyUnknown))
      .recover {
        case _: AskTimeoutException =>
          stageFailCB.invoke(
            new ConsumerFailed(
              s"#$actorNumber Consumer failed during seek for partitions: ${offsets.keys.mkString(", ")}."
            )
          )
      }
  }

  val partitionRevokedCB = getAsyncCallback[Set[TopicPartition]] { revoked =>
    partitionsToRevoke ++= revoked
    scheduleOnce(CloseRevokedPartitions, settings.waitClosePartition)
  }

  override def onTimer(timerKey: Any): Unit = timerKey match {
    case CloseRevokedPartitions =>
      if (log.isDebugEnabled) {
        log.debug("#{} Closing SubSources for revoked partitions: {}", actorNumber, partitionsToRevoke.mkString(", "))
      }
      onRevoke(partitionsToRevoke)
      pendingPartitions --= partitionsToRevoke
      partitionsInStartup --= partitionsToRevoke
      partitionsToRevoke.flatMap(subSources.get).map(_.control).foreach(_.shutdown())
      subSources --= partitionsToRevoke
      partitionsToRevoke = Set.empty
  }

  val subsourceCancelledCB: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])] =
    getAsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])] {
      case (tp, firstUnconsumed) =>
        subSources -= tp
        partitionsInStartup -= tp
        pendingPartitions += tp
        firstUnconsumed match {
          case Some(record) =>
            if (log.isDebugEnabled) {
              log.debug("#{} Seeking {} to {} after partition SubSource cancelled", actorNumber, tp, record.offset())
            }
            seekAndEmitSubSources(formerlyUnknown = Set.empty, Map(tp -> record.offset()))
          case None => emitSubSourcesForPendingPartitions()
        }
    }

  val subsourceStartedCB: AsyncCallback[(TopicPartition, ControlAndStageActor)] =
    getAsyncCallback[(TopicPartition, ControlAndStageActor)] {
      case (tp, value @ ControlAndStageActor(control, _)) =>
        if (!partitionsInStartup.contains(tp)) {
          // Partition was revoked while
          // starting up.  Kill!
          control.shutdown()
        } else {
          subSources += (tp -> value)
          partitionsInStartup -= tp
        }
    }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit =
      emitSubSourcesForPendingPartitions()
    override def onDownstreamFinish(): Unit =
      performShutdown()
  })

  private def updatePendingPartitionsAndEmitSubSources(formerlyUnknownPartitions: Set[TopicPartition]): Unit = {
    pendingPartitions ++= formerlyUnknownPartitions.filter(!partitionsInStartup.contains(_))
    emitSubSourcesForPendingPartitions()
  }

  @tailrec
  private def emitSubSourcesForPendingPartitions(): Unit =
    if (pendingPartitions.nonEmpty && isAvailable(shape.out)) {
      val tp = pendingPartitions.head

      pendingPartitions = pendingPartitions.tail
      partitionsInStartup += tp
      val subSource = Source.fromGraph(
        new SubSourceStage(tp,
                           consumerActor,
                           subsourceStartedCB,
                           subsourceCancelledCB,
                           actorNumber,
                           subSourceStageLogicFactory)
      )
      push(shape.out, (tp, subSource))
      emitSubSourcesForPendingPartitions()
    }

  override def postStop(): Unit = {
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    onShutdown()
    super.postStop()
  }

  override def performStop(): Unit = {
    setKeepGoing(true)
    subSources.foreach {
      case (_, ControlAndStageActor(control, _)) => control.stop()
    }
    complete(shape.out)
    onStop()
  }

  override def performShutdown(): Unit = {
    setKeepGoing(true)
    //todo we should wait for subsources to be shutdown and next shutdown main stage
    subSources.foreach {
      case (_, ControlAndStageActor(control, _)) => control.shutdown()
    }

    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    sourceActor.become {
      case (_, Terminated(ref)) if ref == consumerActor =>
        onShutdown()
        completeStage()
    }
    materializer.scheduleOnce(settings.stopTimeout, new Runnable {
      override def run(): Unit =
        consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    })
  }

  /**
   * Opportunity for subclasses to add their logic to the partition assignment callbacks.
   */
  protected def addToPartitionAssignmentHandler(handler: PartitionAssignmentHandler): PartitionAssignmentHandler =
    handler

}

/** Internal API */
private object SubSourceLogic {
  case object CloseRevokedPartitions
}

/** Internal API
 *
 * A `SubSourceStage` is created per partition in `SubSourceLogic`.
 */
@InternalApi
private final class SubSourceStage[K, V, Msg](
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[(TopicPartition, ControlAndStageActor)],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])],
    actorNumber: Int,
    subSourceStageLogicFactory: SubSourceStageLogicFactory[K, V, Msg]
) extends GraphStage[SourceShape[Msg]] { stage =>

  val out = Outlet[Msg]("out")
  val shape: SourceShape[Msg] = new SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    subSourceStageLogicFactory.create(shape, tp, consumerActor, subSourceStartedCb, subSourceCancelledCb, actorNumber)
}

/** Internal API
 *
 * SubSourceStageLogic [[akka.kafka.scaladsl.Consumer.Control]] and the stage actor [[ActorRef]]
 */
@InternalApi
private final case class ControlAndStageActor(control: Control, stageActor: ActorRef)

/** Internal API
 *
 * Encapsulates a factory method to create a `SubSourceStageLogic` within `SubSourceLogic` where the context
 * parameters exist.
 */
@InternalApi
private trait SubSourceStageLogicFactory[K, V, Msg] {
  def create(
      shape: SourceShape[Msg],
      tp: TopicPartition,
      consumerActor: ActorRef,
      subSourceStartedCb: AsyncCallback[(TopicPartition, ControlAndStageActor)],
      subSourceCancelledCb: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])],
      actorNumber: Int
  ): SubSourceStageLogic[K, V, Msg]
}

/** Internal API
 *
 * A `SubSourceStageLogic` is the `GraphStageLogic` of a SubSourceStage.
 * This emits Kafka messages downstream (not sources).
 */
@InternalApi
private abstract class SubSourceStageLogic[K, V, Msg](
    val shape: SourceShape[Msg],
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[(TopicPartition, ControlAndStageActor)],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])],
    actorNumber: Int
) extends GraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with MessageBuilder[K, V, Msg]
    with StageLogging {
  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = Future.successful(consumerActor)
  private val requestMessages = KafkaConsumerActor.Internal.RequestMessages(0, Set(tp))
  var requested = false
  var subSourceActor: StageActor = _
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  override def preStart(): Unit = {
    log.debug("#{} Starting SubSource for partition {}", actorNumber, tp)
    super.preStart()
    subSourceActor = getStageActor(messageHandling)
    subSourceActor.watch(consumerActor)
    val controlAndActor = ControlAndStageActor(this.asInstanceOf[Control], subSourceActor.ref)
    subSourceStartedCb.invoke(tp -> controlAndActor)
    consumerActor.tell(RegisterSubStage, subSourceActor.ref)
  }

  protected def messageHandling: PartialFunction[(ActorRef, Any), Unit] = {
    case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
      requested = false
      // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
      if (buffer.hasNext) {
        buffer = buffer ++ msg.messages
      } else {
        buffer = msg.messages
      }
      pump()
    case (_, Status.Failure(e)) =>
      failStage(e)
    case (_, Terminated(ref)) if ref == consumerActor =>
      failStage(new ConsumerFailed)
  }

  override def postStop(): Unit = {
    onShutdown()
    super.postStop()
  }

  setHandler(
    shape.out,
    new OutHandler {
      override def onPull(): Unit =
        pump()

      override def onDownstreamFinish(): Unit = {
        val firstUnconsumed = if (buffer.hasNext) {
          Some(buffer.next())
        } else {
          None
        }

        subSourceCancelledCb.invoke(tp -> firstUnconsumed)
        super.onDownstreamFinish()
      }
    }
  )

  def performShutdown() = {
    log.debug("#{} Completing SubSource for partition {}", actorNumber, tp)
    completeStage()
  }

  @tailrec
  private def pump(): Unit =
    if (isAvailable(shape.out)) {
      if (buffer.hasNext) {
        val msg = buffer.next()
        push(shape.out, createMessage(msg))
        pump()
      } else if (!requested) {
        requested = true
        consumerActor.tell(requestMessages, subSourceActor.ref)
      }
    }
}
