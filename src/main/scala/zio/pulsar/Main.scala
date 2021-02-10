package zio.pulsar

import org.apache.pulsar.client.api.{ MessageId, PulsarClient }
import zio._
import zio.json._
import zio.pulsar.client.{ ClientProvider, ClientSettings }
import zio.pulsar.producer.{ ProducerSettings, PulsarProducer }
import zio.pulsar.schema.{ ZSchema }

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

  def produce: ZIO[PulsarProducer[CustomMessage], Throwable, Unit] =
    for {
      f1 <- PulsarProducer.send("key 3", CustomMessage("zio-pulsar-1")).fork
      f2 <- PulsarProducer.send("key 4", CustomMessage("zio-pulsar-2")).fork
      f3 <- PulsarProducer.send("key 5", CustomMessage("zio-pulsar-2")).fork
      _  <- f1.join
      _  <- f2.join
      _  <- f3.join
    } yield ()
}

object Main {
  import Dependencies._

  def main(args: Array[String]): Unit = {

    val client: PulsarClient = PulsarClient
      .builder()
      .serviceUrl("pulsar://localhost:6650")
      .build()

    val producer = client
      .newProducer(zSchema)
      .topic("demo-topic-1")
      .create()

    val consumer = client
      .newConsumer(zSchema)
      .topic("demo-topic-1")
      .subscriptionName("random-consumer-1")
      .subscribe()

    var cnt = 0
    while (true) {
      producer.newMessage().key(cnt.toString).value(CustomMessage(cnt.toString)).send()
      cnt += 1
      val msg = consumer.receive()
      val v   = msg.getValue
      println(v)
    }
  }

}
