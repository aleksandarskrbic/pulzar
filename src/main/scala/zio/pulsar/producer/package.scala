package zio.pulsar

import org.apache.pulsar.client.api.{ MessageId, Producer, PulsarClientException, TypedMessageBuilder }
import zio._
import zio.pulsar.client._
import zio.pulsar.schema.ZSchema

import scala.jdk.CollectionConverters._

package object producer {

  type PulsarProducer[M] = Has[PulsarProducer.Service[M]]

  object PulsarProducer {
    trait Service[M] {
      def underlying: UIO[Producer[M]]
      def sendAsync(key: String, message: M): Task[MessageId]
    }

    final case class Live[M](producer: Producer[M]) extends Service[M] {

      override def underlying: UIO[Producer[M]] = UIO(producer)

      override def sendAsync(key: String, message: M): Task[MessageId] =
        for {
          msg <- UIO(prepare(key, message))
          res <- Task.fromCompletionStage(msg.sendAsync())
        } yield res

      private def prepare(key: String, message: M): TypedMessageBuilder[M] =
        producer.newMessage().key(key).value(message)

      private[producer] def close: UIO[Unit] = UIO(producer.close())
    }

    def make[M](
      producerSettings: ProducerSettings,
      schema: ZSchema[M]
    ): ZManaged[ClientProvider, PulsarClientException, Service[M]] =
      (for {
        client <- ClientProvider.newClient()
        producer <- IO {
                     client
                       .newProducer(schema)
                       .loadConf(producerSettings.toMap.asJava)
                       .create()
                   }.refineToOrDie[PulsarClientException]
      } yield Live(producer)).toManaged(_.close)

    def live[M: Tag](
      producerSettings: ProducerSettings,
      schema: ZSchema[M]
    ): ZLayer[ClientProvider, PulsarClientException, PulsarProducer[M]] =
      make(producerSettings, schema).toLayer

    def sendAsync[M: Tag](key: String, message: M): ZIO[PulsarProducer[M], Throwable, MessageId] =
      ZIO.accessM(_.get.sendAsync(key, message))
  }
}
