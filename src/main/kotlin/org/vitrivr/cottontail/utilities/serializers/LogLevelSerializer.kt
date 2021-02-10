package org.vitrivr.cottontail.utilities.serializers

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.apache.logging.log4j.Level

@ExperimentalSerializationApi
@Serializer(forClass = Level::class)
object LogLevelSerializer : KSerializer<Level> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("LogLevel", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): Level = Level.valueOf(decoder.decodeString())
    override fun serialize(encoder: Encoder, value: Level) {
        encoder.encodeString(value.toString())
    }
}