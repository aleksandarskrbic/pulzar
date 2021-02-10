package zio.pulsar

import org.apache.pulsar.client.api.{ MessageId, PulsarClient }
import zio._
import zio.json._
import zio.pulsar.client.{ ClientProvider, ClientSettings }
import zio.pulsar.producer.{ ProducerSettings, PulsarProducer }
import zio.pulsar.schema.ZSchema

object Dependencies {
  final case class CustomMessage(text: String)
  implicit val encoder: JsonEncoder[CustomMessage] = DeriveJsonEncoder.gen[CustomMessage]
  implicit val decoder: JsonDecoder[CustomMessage] = DeriveJsonDecoder.gen[CustomMessage]

  val zSchema: ZSchema[CustomMessage] = ZSchema[CustomMessage]
}

object ZIOMain extends zio.App {
  import Dependencies._

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val settings         = ClientSettings(List("pulsar://localhost:6650"))
    val clientLayer      = ClientProvider.live(settings)
    val producerSettings = ProducerSettings("demo-topic")
    val producerLayer    = clientLayer >>> PulsarProducer.live(producerSettings, zSchema)

    (produce *> ZIO.succeed(ExitCode.success)).provideLayer(producerLayer).orDie
  }

  def produce: ZIO[PulsarProducer[CustomMessage], Throwable, (MessageId, MessageId)] = {
    for {
      f1 <- PulsarProducer.send("key 1", CustomMessage("zio-pulsar-1")).fork
      f2 <- PulsarProducer.send("key 2", CustomMessage("zio-pulsar-2")).fork
      r1 <- f1.join
      r2 <- f2.join
    } yield (r1, r2)
  }
}

object Main {
  import Dependencies._

  def main(args: Array[String]): Unit = {
    val client: PulsarClient = PulsarClient
      .builder()
      .serviceUrl("pulsar://localhost:6650")
      .build()

    val consumer = client
      .newConsumer(zSchema)
      .topic("demo-topic")
      .subscriptionName("random-consumer-2")
      .subscribe()

    while (true) {
      println("consuming")
      val msg = consumer.batchReceive()
      println(msg)
    }
  }

}
