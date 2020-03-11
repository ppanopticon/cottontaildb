package ch.unibas.dmi.dbis.cottontail.utilities.serializers

import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import java.nio.file.Path
import java.nio.file.Paths

@Serializer(forClass = Path::class)
object PathSerializer : KSerializer<Path> {
    override val descriptor: SerialDescriptor
        get() = PrimitiveDescriptor("Path", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): Path = Paths.get(decoder.decodeString())

    override fun serialize(encoder: Encoder, obj: Path) {
        encoder.encodeString(obj.toString())
    }
}