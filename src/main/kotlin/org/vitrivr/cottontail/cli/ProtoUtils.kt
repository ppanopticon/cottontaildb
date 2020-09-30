package org.vitrivr.cottontail.cli

import com.google.protobuf.Empty
import org.vitrivr.cottontail.grpc.CottontailGrpc

fun Projection(operation: CottontailGrpc.Projection.Operation): CottontailGrpc.Projection = CottontailGrpc.Projection.newBuilder().setOp(operation).build()

fun Where(predicate: CottontailGrpc.AtomicLiteralBooleanPredicate): CottontailGrpc.Where = CottontailGrpc.Where.newBuilder().setAtomic(predicate).build()

fun MatchAll(): CottontailGrpc.Projection = CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.SELECT).putAttributes("*", "").build()

fun Empty(): Empty = Empty.getDefaultInstance()