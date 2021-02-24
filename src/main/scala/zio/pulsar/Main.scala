package zio.pulsar

import zio._
import zio.clock.Clock
import zio.json._
import zio.console._
import zio.pulsar.client.{ClientProvider, ClientSettings}
import zio.pulsar.consumer.{ConsumerSettings, PulsarConsumer}
import zio.pulsar.producer.{ProducerSettings, PulsarProducer}
import zio.pulsar.schema.ZSchema
import zio.stream.ZStream

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
    val producerSettings = ProducerSettings("zstream-topic")
    val producerLayer    = clientLayer >>> PulsarProducer.live(producerSettings, zSchema)
    val consumerSettings = ConsumerSettings("zstream-topic")
    val consumerLayer    = clientLayer >>> PulsarConsumer.live(consumerSettings, zSchema, "zio-consumer-111")
    val pulsarLayer      = producerLayer ++ consumerLayer

    program.provideCustomLayer(pulsarLayer).orDie.exitCode
  }

  val program =
    for {
      _ <- produce
      _ <- PulsarConsumer.plainStream[CustomMessage]
        .mapM(msg => putStrLn(msg.getValue.text) *> UIO(msg.getMessageId))
        .mapM(msgId => PulsarConsumer.negativeAcknowledge[CustomMessage](msgId))
        .runDrain
    } yield ()

  def produce: ZIO[PulsarProducer[CustomMessage], Throwable, Unit] =
    for {
      _ <- PulsarProducer.send("key 1", CustomMessage("1"))
      _ <- PulsarProducer.send("key 2", CustomMessage("2"))
      _ <- PulsarProducer.send("key 3", CustomMessage("3"))
    } yield ()

  def consume: ZIO[Console with PulsarConsumer[CustomMessage], Throwable, Unit] =
    (for {
      message <- PulsarConsumer.receive[CustomMessage]
      _       <- putStrLn(message.getValue.text)
    } yield ()).forever
}