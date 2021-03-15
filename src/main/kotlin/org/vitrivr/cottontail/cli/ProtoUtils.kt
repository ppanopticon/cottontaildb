package org.vitrivr.cottontail.cli

import com.google.protobuf.Empty
import org.vitrivr.cottontail.grpc.CottontailGrpc

fun Projection(operation: CottontailGrpc.Projection.ProjectionOperation): CottontailGrpc.Projection = CottontailGrpc.Projection.newBuilder().setOp(operation).build()

fun Where(predicate: CottontailGrpc.AtomicBooleanPredicate): CottontailGrpc.Where = CottontailGrpc.Where.newBuilder().setAtomic(predicate).build()

fun MatchAll(): CottontailGrpc.Projection = CottontailGrpc.Projection.newBuilder()
    .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
    .addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*")).build())
    .build()

fun Empty(): Empty = Empty.getDefaultInstance()