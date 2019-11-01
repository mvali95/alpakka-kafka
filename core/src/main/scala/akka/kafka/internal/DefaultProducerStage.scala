/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import akka.kafka.internal.ProducerStage.{MessageCallback, ProducerCompletionState}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.{Attributes, FlowShape, Supervision}
import akka.stream.stage._
import org.apache.kafka.clients.producer.{Callback, Producer, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] class DefaultProducerStage[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
    val settings: ProducerSettings[K, V]
) extends GraphStage[FlowShape[IN, Future[OUT]]]
    with ProducerStage[K, V, P, IN, OUT] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new DefaultProducerStageLogic(this, inheritedAttributes)
}

private[kafka] object DefaultProducerStageLogic {
  sealed trait ProducerAssignmentLifecycle
  case object Unassigned extends ProducerAssignmentLifecycle
  case object AsyncCreateRequestSent extends ProducerAssignmentLifecycle
  case object Assigned extends ProducerAssignmentLifecycle
}

/**
 * Internal API.
 *
 * Used by [[DefaultProducerStage]], extended by [[TransactionalProducerStageLogic]].
 */
private class DefaultProducerStageLogic[K, V, P, IN <: Envelope[K, V, P], OUT <: Results[K, V, P]](
    stage: ProducerStage[K, V, P, IN, OUT],
    inheritedAttributes: Attributes
) extends TimerGraphStageLogic(stage.shape)
    with StageLogging
    with MessageCallback[K, V, P]
    with ProducerCompletionState {

  import DefaultProducerStageLogic._

  private lazy val decider: Decider =
    inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
  protected val awaitingConfirmation = new AtomicInteger(0)
  protected var producer: Producer[K, V] = _
  protected var producerAssignmentLifecycle: ProducerAssignmentLifecycle = Unassigned
  private var inIsClosed = false
  private var completionState: Option[Try[Done]] = None

  override protected def logSource: Class[_] = classOf[DefaultProducerStage[_, _, _, _, _]]

  override def preStart(): Unit = {
    super.preStart()
    resolveProducer(stage.settings)
  }

  protected def changeProducerAssignmentLifecycle(state: ProducerAssignmentLifecycle): Unit = {
    val oldState = producerAssignmentLifecycle
    producerAssignmentLifecycle = state
    log.debug("Asynchronous producer assignment lifecycle changed '{} -> {}'", oldState, state)
  }

  protected def assignProducer(p: Producer[K, V]): Unit = {
    producer = p
    changeProducerAssignmentLifecycle(Assigned)
    resumeDemand()
  }

  protected def resolveProducer(settings: ProducerSettings[K, V]): Unit = {
    val producerFuture = settings.createKafkaProducerAsync()(materializer.executionContext)
    producerFuture.value match {
      case Some(Success(p)) => assignProducer(p)
      case Some(Failure(e)) => failStage(e)
      case None =>
        val assign = getAsyncCallback(assignProducer)
        producerFuture
          .transform(
            producer => assign.invoke(producer),
            e => {
              log.error(e, "producer creation failed")
              failStageCb.invoke(e)
              e
            }
          )(ExecutionContexts.sameThreadExecutionContext)
        changeProducerAssignmentLifecycle(AsyncCreateRequestSent)
    }
  }

  def checkForCompletion(): Unit =
    if (isClosed(stage.in) && awaitingConfirmation.get == 0) {
      completionState match {
        case Some(Success(_)) => onCompletionSuccess()
        case Some(Failure(ex)) => onCompletionFailure(ex)
        case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
      }
    }

  override def onCompletionSuccess(): Unit = completeStage()

  override def onCompletionFailure(ex: Throwable): Unit = failStage(ex)

  val checkForCompletionCB: AsyncCallback[Unit] = getAsyncCallback[Unit] { _ =>
    checkForCompletion()
  }

  val failStageCb: AsyncCallback[Throwable] = getAsyncCallback[Throwable] { ex =>
    if (producer != null) {
      // Discard unsent ProducerRecords after encountering a send-failure in ProducerStage
      // https://github.com/akka/alpakka-kafka/pull/318
      producer.close(0L, TimeUnit.MILLISECONDS)
    }
    failStage(ex)
  }

  def filterSend(msg: Envelope[K, V, P]): Boolean = true

  def postSend(msg: Envelope[K, V, P]): Unit = ()

  protected def resumeDemand(tryToPull: Boolean = true): Unit = {
    log.debug("Resume demand")
    setHandler(stage.out, new OutHandler {
      override def onPull(): Unit = tryPull(stage.in)
    })
    // kick off demand for more messages if we're resuming demand
    if (tryToPull && isAvailable(stage.out) && !hasBeenPulled(stage.in)) {
      tryPull(stage.in)
    }
  }

  protected def suspendDemand(fromStageLogicConstructor: Boolean = false): Unit = {
    // not permitted to access stage logic members from constructor
    if (!fromStageLogicConstructor) log.debug("Suspend demand")
    setHandler(
      stage.out,
      new OutHandler {
        override def onPull(): Unit = ()
      }
    )
  }

  // suspend demand until a Producer has been created
  suspendDemand(fromStageLogicConstructor = true)

  setHandler(
    stage.in,
    new InHandler {
      override def onPush(): Unit = {
        val msg = grab(stage.in)
        if (filterSend(msg))
          produce(msg)
      }

      override def onUpstreamFinish(): Unit = {
        inIsClosed = true
        completionState = Some(Success(Done))
        checkForCompletion()
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        inIsClosed = true
        completionState = Some(Failure(ex))
        checkForCompletion()
      }
    }
  )

  def produce(in: Envelope[K, V, P]): Unit =
    in match {
      case msg: Message[K, V, P] =>
        val r = Promise[Result[K, V, P]]
        awaitingConfirmation.incrementAndGet()
        producer.send(msg.record, sendCallback(r, onSuccess = metadata => {
          r.success(Result(metadata, msg))
        }))
        postSend(msg)
        val future = r.future.asInstanceOf[Future[OUT]]
        push(stage.out, future)

      case multiMsg: MultiMessage[K, V, P] =>
        val promises = for {
          msg <- multiMsg.records
        } yield {
          val r = Promise[MultiResultPart[K, V]]
          awaitingConfirmation.incrementAndGet()
          producer.send(msg, sendCallback(r, onSuccess = metadata => r.success(MultiResultPart(metadata, msg))))
          r.future
        }
        postSend(multiMsg)
        implicit val ec: ExecutionContext = this.materializer.executionContext
        val res = Future.sequence(promises).map { parts =>
          MultiResult(parts, multiMsg.passThrough)
        }
        val future = res.asInstanceOf[Future[OUT]]
        push(stage.out, future)

      case passthrough: PassThroughMessage[K, V, P] =>
        postSend(passthrough)
        val future = Future.successful(PassThroughResult[K, V, P](in.passThrough)).asInstanceOf[Future[OUT]]
        push(stage.out, future)

    }

  private def sendCallback(promise: Promise[_], onSuccess: RecordMetadata => Unit): Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) onSuccess(metadata)
      else
        decider(exception) match {
          case Supervision.Stop => failStageCb.invoke(exception)
          case _ => promise.failure(exception)
        }
      if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
        checkForCompletionCB.invoke(())
    }
  }

  override def postStop(): Unit = {
    log.debug("ProducerStage completed")
    closeProducer()
    super.postStop()
  }

  private def closeProducer(): Unit =
    if (stage.settings.closeProducerOnStop && producerAssignmentLifecycle == Assigned) {
      try {
        // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
        producer.flush()
        producer.close(stage.settings.closeTimeout.toMillis, TimeUnit.MILLISECONDS)
        log.debug("Producer closed")
      } catch {
        case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
      }
    }

}
