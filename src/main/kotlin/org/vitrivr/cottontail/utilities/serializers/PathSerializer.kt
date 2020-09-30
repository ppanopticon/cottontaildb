package org.vitrivr.cottontail.utilities.serializers

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.nio.file.Path
import java.nio.file.Paths

@ExperimentalSerializationApi
@Serializer(forClass = Path::class)
object PathSerializer : KSerializer<Path> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("yourSerializerUniqueName", PrimitiveKind.STRING)
    override fun deserialize(decoder: Decoder): Path = Paths.get(decoder.decodeString())
    override fun serialize(encoder: Encoder, value: Path) {
        encoder.encodeString(value.toString())
    }
}