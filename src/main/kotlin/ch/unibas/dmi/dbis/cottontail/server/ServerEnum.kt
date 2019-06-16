package ch.unibas.dmi.dbis.cottontail.server

/**
 * Enumeration that specifies, what kind of [Server] should be instantiated.
 *
 * @author Ralph Gasser
 * @param 1.0
 */
enum class ServerEnum {
    AVATICA,
    GRPC
}