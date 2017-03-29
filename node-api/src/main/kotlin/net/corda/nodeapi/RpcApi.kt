package net.corda.nodeapi

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.pool.KryoPool
import net.corda.core.ErrorOr
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.serialization.serializeToStream
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.reader.MessageUtil
import java.io.InputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.util.concurrent.ExecutorService

fun ClientMessage.streamBody(executor: ExecutorService): InputStream {
    val inputStream = PipedInputStream()
    val outputStream = PipedOutputStream(inputStream)
    executor.submit {
        setOutputStream(outputStream)
    }
    return inputStream
}

fun <T : Any> T.streamToClientMessageBody(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
    val outputStream = PipedOutputStream()
    val inputStream = PipedInputStream(outputStream)
    message.setBodyInputStream(inputStream)
    executor.submit {
        try {
            kryoPool.run { kryo -> serializeToStream(kryo, outputStream) }
        } catch(t : Throwable) {
            println("ehh $t")
            throw t
        }
    }
}

object RpcApi {
    private val TAG_FIELD_NAME = "tag"
    private val RPC_ID_FIELD_NAME = "rpc-id"
    private val OBSERVABLE_ID_FIELD_NAME = "observable-id"
    private val METHOD_NAME_FIELD_NAME = "method-name"
    private val OBSERVABLE_CLOSED_EXCEPTION_FIELD_NAME = "observable-close-exception"

    val RPC_SERVER_QUEUE_NAME = "rpc-server"
    val RPC_CLIENT_QUEUE_NAME_PREFIX = "rpc-client"

    data class RpcClientId(val toLong: Long)
    data class RpcRequestId(val toLong: Long)
    data class ObservableId(val toLong: Long)

    sealed class ClientToServer {
        private enum class Tag {
            RPC_REQUEST,
            OBSERVABLE_CLOSED
        }

        // Visitor a'la Church
        abstract fun <A> accept(
                onRpcRequest: (RpcRequest) -> A,
                onObservableClosed: (ObservableClosed) -> A
        ): A

        data class RpcRequest(
                val clientAddress: SimpleString,
                val id: RpcRequestId,
                val methodName: String,
                val arguments: List<Any?>
        ) : ClientToServer() {
            override fun <A> accept(onRpcRequest: (RpcRequest) -> A, onObservableClosed: (ObservableClosed) -> A): A {
                return onRpcRequest(this)
            }

            fun writeToClientMessage(executor: ExecutorService, kryo: KryoPool, message: ClientMessage) {
                MessageUtil.setJMSReplyTo(message, clientAddress)
                message.putIntProperty(TAG_FIELD_NAME, Tag.RPC_REQUEST.ordinal)
                message.putLongProperty(RPC_ID_FIELD_NAME, id.toLong)
                message.putStringProperty(METHOD_NAME_FIELD_NAME, methodName)
                arguments.streamToClientMessageBody(executor, kryo, message)
            }
        }

        data class ObservableClosed(
                val id: ObservableId
        ) : ClientToServer() {
            override fun <A> accept(onRpcRequest: (RpcRequest) -> A, onObservableClosed: (ObservableClosed) -> A): A {
                return onObservableClosed(this)
            }

            fun writeToClientMessage(message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.OBSERVABLE_CLOSED.ordinal)
                message.putLongProperty(OBSERVABLE_ID_FIELD_NAME, id.toLong)
            }
        }

        companion object {
            fun fromClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage): ClientToServer {
                val tag = Tag.values()[message.getIntProperty(TAG_FIELD_NAME)]
                return when (tag) {
                    RpcApi.ClientToServer.Tag.RPC_REQUEST -> RpcRequest(
                            clientAddress = MessageUtil.getJMSReplyTo(message),
                            id = RpcRequestId(message.getLongProperty(RPC_ID_FIELD_NAME)),
                            methodName = message.getStringProperty(METHOD_NAME_FIELD_NAME),
                            arguments = message.streamBody(executor).deserialize(kryoPool)
                    )
                    RpcApi.ClientToServer.Tag.OBSERVABLE_CLOSED -> ObservableClosed(
                            id = ObservableId(message.getLongProperty(OBSERVABLE_ID_FIELD_NAME))
                    )
                }
            }
        }
    }

    sealed class ServerToClient {
        private enum class Tag {
            RPC_REPLY,
            OBSERVATION,
            OBSERVABLE_CLOSED
        }

        abstract fun <A> accept(
                onRpcReply: (RpcReply) -> A,
                onObservation: (Observation) -> A,
                onObservableClosed: (ObservableClosed) -> A
        ): A

        abstract fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage)

        data class RpcReply(
                val id: RpcRequestId,
                val result: ErrorOr<Any>
        ) : ServerToClient() {
            override fun <A> accept(onRpcReply: (RpcReply) -> A, onObservation: (Observation) -> A, onObservableClosed: (ObservableClosed) -> A): A {
                return onRpcReply(this)
            }

            override fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.RPC_REPLY.ordinal)
                message.putLongProperty(RPC_ID_FIELD_NAME, id.toLong)
                result.streamToClientMessageBody(executor, kryoPool, message)
            }
        }

        data class Observation(
                val id: ObservableId,
                val content: Any
        ) : ServerToClient() {
            override fun <A> accept(onRpcReply: (RpcReply) -> A, onObservation: (Observation) -> A, onObservableClosed: (ObservableClosed) -> A): A {
                return onObservation(this)
            }

            override fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.OBSERVATION.ordinal)
                message.putLongProperty(OBSERVABLE_ID_FIELD_NAME, id.toLong)
                content.streamToClientMessageBody(executor, kryoPool, message)
            }
        }

        data class ObservableClosed(
                val id: ObservableId,
                val exception: Throwable? // null if clean shutdown
        ) : ServerToClient() {
            override fun <A> accept(onRpcReply: (RpcReply) -> A, onObservation: (Observation) -> A, onObservableClosed: (ObservableClosed) -> A): A {
                return onObservableClosed(this)
            }

            override fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.OBSERVABLE_CLOSED.ordinal)
                message.putLongProperty(OBSERVABLE_ID_FIELD_NAME, id.toLong)
                exception?.let { message.putBytesProperty(OBSERVABLE_CLOSED_EXCEPTION_FIELD_NAME, it.serialize(kryoPool).bytes) }
            }
        }

        companion object {
            fun fromClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage): ServerToClient {
                val tag = Tag.values()[message.getIntProperty(TAG_FIELD_NAME)]
                return when (tag) {
                    RpcApi.ServerToClient.Tag.RPC_REPLY -> RpcReply(
                            id = RpcRequestId(message.getLongProperty(RPC_ID_FIELD_NAME)),
                            result = message.streamBody(executor).deserialize(kryoPool)
                    )
                    RpcApi.ServerToClient.Tag.OBSERVATION -> Observation(
                            id = ObservableId(message.getLongProperty(OBSERVABLE_ID_FIELD_NAME)),
                            content = message.streamBody(executor).deserialize(kryoPool)
                    )
                    RpcApi.ServerToClient.Tag.OBSERVABLE_CLOSED -> ObservableClosed(
                            id = ObservableId(message.getLongProperty(OBSERVABLE_ID_FIELD_NAME)),
                            exception = message.getBytesProperty(OBSERVABLE_CLOSED_EXCEPTION_FIELD_NAME)?.deserialize(kryoPool)
                    )
                }
            }
        }
    }
}