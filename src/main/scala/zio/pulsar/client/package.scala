package zio.pulsar

import zio._
import org.apache.pulsar.client.api.{ PulsarClient, PulsarClientException }

import scala.jdk.CollectionConverters._

package object client {

  type ClientProvider = Has[ClientProvider.Service]

  object ClientProvider {
    trait Service {
      def newClient(): UIO[PulsarClient]
    }

    final case class Live(client: PulsarClient) extends Service {
      override def newClient(): UIO[PulsarClient] = UIO(client)

      private[client] def close: UIO[Unit] = UIO(client.close())
    }

    def make(settings: ClientSettings): ZManaged[Any, PulsarClientException, Service] =
      (for {
        client <- IO {
                   PulsarClient
                     .builder()
                     .loadConf(settings.toMap.asJava)
                     .build()
                 }.refineToOrDie[PulsarClientException]
      } yield Live(client)).toManaged(_.close)

    def live(settings: ClientSettings): ZLayer[Any, PulsarClientException, ClientProvider] =
      make(settings).toLayer

    def newClient(): URIO[ClientProvider, PulsarClient] =
      ZIO.accessM(env => env.get.newClient())
  }
}
