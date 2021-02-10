package org.vitrivr.cottontail.utilities.data.exporter

import com.google.gson.stream.JsonWriter
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.utilities.data.Format
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * A [DataExporter] implementation that can be used to write a [Format.JSON] files containing an array of entries.
 *
 * Data produced by the [JsonDataExporter] can be used as input for the [JsonDataImporter].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class JsonDataExporter(override val path: Path, val indent: String = "") : DataExporter {
    /** The [Format] handled by this [DataImporter]. */
    override val format: Format = Format.JSON

    /** Indicator whether this [DataImporter] has been closed. */
    override var closed: Boolean = false
        private set

    /** The [JsonWriter] instance used to read the JSON file. */
    private val writer = JsonWriter(
        Files.newBufferedWriter(
            this.path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE
        )
    )

    init {
        this.writer.setIndent(this.indent)
        this.writer.beginArray() /* Starts writer the JSON array, which is the expected input. */
    }

    /**
     * Offers a new [CottontailGrpc.QueryResponseMessage] to this [ProtoDataExporter], which will
     * be appended to the output stream.
     *
     * @param message The [CottontailGrpc.QueryResponseMessage] to append.
     */
    override fun offer(message: CottontailGrpc.QueryResponseMessage) {
        for (tuple in message.tuplesList) {
            this.writer.beginObject()
            tuple.dataList.zip(message.columnsList).forEach { e ->
                this.writer.name(e.second.name)
                when (e.first.dataCase) {
                    CottontailGrpc.Literal.DataCase.BOOLEANDATA -> this.writer.value(e.first.booleanData)
                    CottontailGrpc.Literal.DataCase.INTDATA -> this.writer.value(e.first.intData)
                    CottontailGrpc.Literal.DataCase.LONGDATA -> this.writer.value(e.first.longData)
                    CottontailGrpc.Literal.DataCase.FLOATDATA -> this.writer.value(e.first.floatData)
                    CottontailGrpc.Literal.DataCase.DOUBLEDATA -> this.writer.value(e.first.doubleData)
                    CottontailGrpc.Literal.DataCase.STRINGDATA -> this.writer.value(e.first.stringData)
                    CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> this.writeComplex(
                        e.first.complex32Data.real,
                        e.first.complex32Data.imaginary
                    )
                    CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> this.writeComplex(
                        e.first.complex64Data.real,
                        e.first.complex64Data.imaginary
                    )
                    CottontailGrpc.Literal.DataCase.VECTORDATA -> {
                        this.writer.beginArray()
                        when (e.first.vectorData.vectorDataCase) {
                            CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> {
                                e.first.vectorData.floatVector.vectorList.forEach {
                                    this.writer.value(
                                        it
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> {
                                e.first.vectorData.doubleVector.vectorList.forEach {
                                    this.writer.value(
                                        it
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> {
                                e.first.vectorData.intVector.vectorList.forEach {
                                    this.writer.value(
                                        it
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> {
                                e.first.vectorData.longVector.vectorList.forEach {
                                    this.writer.value(
                                        it
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> {
                                e.first.vectorData.boolVectorOrBuilder.vectorList.forEach {
                                    this.writer.value(
                                        it
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> {
                                e.first.vectorData.boolVectorOrBuilder.vectorList.forEach {
                                    this.writeComplex(
                                        e.first.complex32Data.real,
                                        e.first.complex32Data.imaginary
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> {
                                e.first.vectorData.boolVectorOrBuilder.vectorList.forEach {
                                    this.writeComplex(
                                        e.first.complex64Data.real,
                                        e.first.complex64Data.imaginary
                                    )
                                }
                            }
                            CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET,
                            null -> {
                            }
                        }
                        this.writer.endArray()
                    }
                    CottontailGrpc.Literal.DataCase.NULLDATA,
                    CottontailGrpc.Literal.DataCase.DATA_NOT_SET,
                    null -> this.writer.nullValue()
                }
            }
            this.writer.endObject()
        }
    }

    /**
     * Writes a complex number to the [JsonWriter].
     *
     * @param real The real part of the number.
     * @param imaginary The imaginary part of the number.
     */
    private fun writeComplex(real: Number, imaginary: Number) {
        this.writer.beginObject()
        this.writer.name("real")
        this.writer.value(real)
        this.writer.name("imaginary")
        this.writer.value(imaginary)
        this.writer.endObject()
    }

    /**
     * Closes this [ProtoDataExporter].
     */
    override fun close() {
        if (!this.closed) {
            this.writer.endArray()
            this.writer.close()
            this.closed = true
        }
    }
}