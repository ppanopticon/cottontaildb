package org.vitrivr.cottontail.utilities.extensions

inline fun <T> Collection<T>.forEachP(action: (T) -> Unit): Unit {
    for (i in 0 until size) {
        action(elementAt(i))
    }
}

inline fun <K, V> Map<out K, V>.forEachP(action: (Map.Entry<K, V>) -> Unit): Unit {
    for (i in 0 until size) {
        action(entries.elementAt(i))
    }
}

inline fun <T> List<T>.firstOrNullP(predicate: (T) -> Boolean): T? {
    for (i in 0 until size) {
        val element = get(i)
        if (predicate(element)) return element
    }
    return null
}

inline fun <T> List<T>.filterP(predicate: (T) -> Boolean): List<T> {
    val destination = ArrayList<T>(size)
    for (i in 0 until size) {
        val element = get(i)
        if (predicate(element)) destination.add(element)
    }
    return destination
}
