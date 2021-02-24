package zio.pulsar

import org.apache.pulsar.client.api.{ Consumer, Message, MessageId, PulsarClientException }
import zio._
import zio.pulsar.client.ClientProvider
import zio.pulsar.schema.ZSchema
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

package object consumer {

  type PulsarConsumer[M] = Has[PulsarConsumer.Service[M]]

  object PulsarConsumer {
    trait Service[M] {
      def receive: Task[Message[M]]

      def acknowledge(messageId: MessageId): Task[Unit]

      def negativeAcknowledge(messageId: MessageId): UIO[Unit]

      def plainStream: ZStream[Any, Throwable, Message[M]]
    }

    case class Live[M](consumer: Consumer[M], queue: Queue[Message[M]]) extends Service[M] {

      override def receive: Task[Message[M]] =
        Task.fromCompletionStage(consumer.receiveAsync())

      override def acknowledge(messageId: MessageId): Task[Unit] =
        Task.fromCompletionStage(consumer.acknowledgeAsync(messageId)).unit

      override def negativeAcknowledge(messageId: MessageId): UIO[Unit] =
        UIO(consumer.negativeAcknowledge(messageId))

      override def plainStream: ZStream[Any, Throwable, Message[M]] =
        ZStream.repeatEffect(receive)

      private[consumer] def close: UIO[Unit] =
        UIO(consumer.close())
    }

    def make[M](
      consumerSettings: ConsumerSettings,
      schema: ZSchema[M],
      sub: String
    ): ZManaged[ClientProvider, PulsarClientException, Service[M]] =
      (for {
        client <- ClientProvider.newClient()
        consumer <- IO {
                     client
                       .newConsumer(schema)
                       .topic(consumerSettings.topics)
                       .subscriptionName(sub)
                       .loadConf(consumerSettings.properties.asJava)
                       .subscribe()
                   }.refineToOrDie[PulsarClientException]
        queue <- Queue.unbounded[Message[M]]
      } yield Live(consumer, queue)).toManaged(_.close)

    def live[M: Tag](
      consumerSettings: ConsumerSettings,
      schema: ZSchema[M],
      sub: String
    ): ZLayer[ClientProvider, PulsarClientException, PulsarConsumer[M]] =
      make(consumerSettings, schema, sub).toLayer

    def receive[M: Tag]: ZIO[PulsarConsumer[M], Throwable, Message[M]] =
      ZIO.accessM(_.get.receive)

    def acknowledge[M: Tag](messageId: MessageId): ZIO[PulsarConsumer[M], Throwable, Unit] =
      ZIO.accessM(_.get.acknowledge(messageId))

    def negativeAcknowledge[M: Tag](messageId: MessageId): ZIO[PulsarConsumer[M], Throwable, Unit] =
      ZIO.accessM(_.get.negativeAcknowledge(messageId))

    def plainStream[M: Tag]: ZStream[PulsarConsumer[M], Throwable, Message[M]] =
      ZStream.accessStream(_.get.plainStream)
  }
}
