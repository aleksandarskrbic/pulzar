package zio.pulsar.schema

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{ SchemaInfo, SchemaType }
import zio.json._

class ZSchema[T: Manifest](implicit encoder: JsonEncoder[T], decoder: JsonDecoder[T]) extends Schema[T] {
  override def encode(message: T): Array[Byte] =
    encoder.encodeJson(message, Option(4)).toString.getBytes("UTF-8")

  override def decode(bytes: Array[Byte]): T =
    decoder.decodeJson(new String(bytes)).fold(error => throw new RuntimeException(error), identity)

  override def getSchemaInfo: SchemaInfo =
    new SchemaInfo()
      .setName(manifest[T].runtimeClass.getCanonicalName)
      .setType(SchemaType.JSON)
      .setSchema("""{"type":"any"}""".getBytes("UTF-8"))
}

object ZSchema {

  def apply[T: Manifest](implicit encoder: JsonEncoder[T], decoder: JsonDecoder[T]): ZSchema[T] =
    new ZSchema[T]()
}