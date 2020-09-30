package org.vitrivr.cottontail.storage.basics

data class MemorySize(val value: Double, val unit: Units = Units.BYTE) : Comparable<MemorySize>{
    infix fun `in`(unit: Units) = MemorySize(unit.convert(this.value, this.unit), unit)
    override fun toString(): String = "${this.value}${unit.symbol}"
    override fun compareTo(other: MemorySize): Int = (this `in` Units.BYTE).value.compareTo((other `in` Units.BYTE).value)
}

enum class Units(val factor: Double, val symbol: String) {
    BYTE(1e0, "B"),
    KILOBYTE(1e3, "KB"),
    MEGABYTE(1e6, "MB"),
    GIGABYTE(1e9, "GB"),
    PETABYTE(1e12, "PB");
    fun convert(value: Double, unit: Units) = ((value * unit.factor)/this.factor)
}