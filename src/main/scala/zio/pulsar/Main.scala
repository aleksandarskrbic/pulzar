package zio.pulsar

import org.apache.pulsar.client.api.{ MessageId, PulsarClient }
import zio._
import zio.json._
import zio.console._
import zio.pulsar.client.{ ClientProvider, ClientSettings }
import zio.pulsar.consumer.{ ConsumerSettings, PulsarConsumer }
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
    val producerSettings = ProducerSettings("zstream-topic")
    val producerLayer    = clientLayer >>> PulsarProducer.live(producerSettings, zSchema)
    val consumerSettings = ConsumerSettings("zstream-topic")
    val consumerLayer    = clientLayer >>> PulsarConsumer.live(consumerSettings, zSchema, "zio-consumer")
    val pulsarLayer      = producerLayer ++ consumerLayer

    program.provideCustomLayer(pulsarLayer).orDie.exitCode
  }

  val program =
    for {
      _ <- produce
      _ <- PulsarConsumer.plainStream[CustomMessage]
        .mapM(msg => putStrLn(msg.getValue.text) *> UIO(msg.getMessageId))
        .mapM(msgId => PulsarConsumer.acknowledge[CustomMessage](msgId))
        .runDrain
    } yield ()

  /*  val program: ZIO[Console with PulsarConsumer[CustomMessage] with PulsarProducer[CustomMessage], Throwable, ExitCode] =
    for {
      _ <- produce
      _ <- consume
    } yield ExitCode.success*/

  def produce: ZIO[PulsarProducer[CustomMessage], Throwable, Unit] =
    for {
      f1 <- PulsarProducer.send("key 1", CustomMessage("zio-pulsar-1")).fork
      f2 <- PulsarProducer.send("key 2", CustomMessage("zio-pulsar-2")).fork
      f3 <- PulsarProducer.send("key 3", CustomMessage("zio-pulsar-3")).fork
      _  <- f1.join
      _  <- f2.join
      _  <- f3.join
    } yield ()

  def consume: ZIO[Console with PulsarConsumer[CustomMessage], Throwable, Unit] =
    (for {
      message <- PulsarConsumer.receive[CustomMessage]
      _       <- putStrLn(message.getValue.text)
    } yield ()).forever
}