package net.corda.client.rpc

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.pool.KryoPool
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.SettableFuture
import net.corda.core.ThreadBox
import net.corda.core.messaging.RPCOps
import net.corda.core.random63BitValue
import net.corda.core.serialization.KryoPoolWithContext
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.ArtemisTcpTransport.Companion.tcpTransport
import net.corda.nodeapi.ConnectionDirection
import net.corda.nodeapi.RPCKryo
import net.corda.nodeapi.RpcApi
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientProducer
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.slf4j.LoggerFactory
import rx.Observable
import rx.subjects.PublishSubject
import java.lang.ref.WeakReference
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

private data class RpcObservableMap(
        val map: ConcurrentHashMap<RpcApi.ObservableId, WeakReference<PublishSubject<Any>>>
)

private val clientAddress = SimpleString("${RpcApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.${random63BitValue()}")

class CordaRpcProxyHandler(
        private val executor: ExecutorService,
        session: ClientSession,
        producer: ClientProducer,
        consumer: ClientConsumer
) : InvocationHandler {

    private val artemisState = ThreadBox(object {
        val session = session
        val producer = producer
        val consumer = consumer
    })

    private val rpcReplyMap = ConcurrentHashMap<RpcApi.RpcRequestId, SettableFuture<Any>>()
    private val observableMap = RpcObservableMap(ConcurrentHashMap())
    private val kryoPoolWithObservableContext = KryoPoolWithContext(kryoPool, RpcObservableMapKey, observableMap)
    init {
        consumer.setMessageHandler {
            val serverToClient = RpcApi.ServerToClient.fromClientMessage(executor, kryoPoolWithObservableContext, it)
            log.info("Got message from RPC server $serverToClient")
            serverToClient.accept(
                    onRpcReply = {
                        val replyFuture = rpcReplyMap.remove(it.id)
                        if (replyFuture == null) {
                            log.warn("RPC reply arrived to unknown RPC ID ${it.id}")
                            return@accept
                        }
                        it.result.match(
                                onError = { replyFuture.setException(it) },
                                onValue = { replyFuture.set(it) }
                        )
                    },
                    onObservation = {
                        val observable = observableMap.map.get(it.id)
                        if (observable == null) {
                            log.warn("Observation arrived to unknown Observable with ID ${it.id}")
                            return@accept
                        }
                        observable.get()?.onNext(it.content)
                    },
                    onObservableClosed = {
                        val observable = observableMap.map.remove(it.id)
                        if (observable == null) {
                            log.debug("Observable closed on server side, but it's absent on client, ID ${it.id}")
                            return@accept
                        }
                        observable.get()?.onCompleted()
                    }
            )
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(CordaRPCClient2::class.java)
        private object RpcObservableMapKey
        private val observableSerializer = object : Serializer<Observable<Any>>() {
            override fun read(kryo: Kryo, input: Input, type: Class<Observable<Any>>): Observable<Any> {
                @Suppress("UNCHECKED_CAST")
                val observableMap = kryo.context[RpcObservableMapKey] as RpcObservableMap
                val observableId = RpcApi.ObservableId(input.readLong(true))
                val observable = PublishSubject.create<Any>()
                require(observableMap.map.put(observableId, WeakReference(observable)) == null) {
                    "Multiple Observables arrived with the same ID $observableId"
                }
                return observable
            }
            override fun write(kryo: Kryo, output: Output, observable: Observable<Any>) {
                throw UnsupportedOperationException("Cannot serialise Observables on the client side")
            }
        }
        private val kryoPool = KryoPool.Builder { RPCKryo(observableSerializer) }.build()
    }

    override fun invoke(proxy: Any, method: Method, arguments: Array<out Any?>?): Any? {
        val rpcId = RpcApi.RpcRequestId(random63BitValue())
        val request = RpcApi.ClientToServer.RpcRequest(clientAddress, rpcId, method.name, arguments?.toList() ?: emptyList())
        val message = artemisState.locked { session.createMessage(false) }
        request.writeToClientMessage(executor, kryoPool, message)
        log.info("Sending RPC request ${method.name}")
        val replyFuture = SettableFuture.create<Any>()
        require(rpcReplyMap.put(rpcId, replyFuture) == null) {
            "Generated several RPC requests with same ID $rpcId"
        }
        artemisState.locked { producer.send(message) }
        return replyFuture.get()
    }
}

class CordaRPCClient2 {

    companion object {
        private val executor = Executors.newCachedThreadPool()
        private val log = loggerFor<CordaRPCClient2>()

        fun <I : RPCOps> connect(rpcOpsClass: Class<I>, hostAndPort: HostAndPort, username: String, password: String): I {
            val transport = tcpTransport(ConnectionDirection.Outbound(), hostAndPort, null, false)
            val serverLocator = ActiveMQClient.createServerLocatorWithoutHA(transport)
            val sessionFactory = serverLocator.createSessionFactory()
            val session = sessionFactory.createSession(username, password, false, true, true, serverLocator.isPreAcknowledge, serverLocator.ackBatchSize)
            session.start()
            val producer = session.createProducer(RpcApi.RPC_SERVER_QUEUE_NAME)
            session.createQueue(clientAddress, clientAddress, false)
            val consumer = session.createConsumer(clientAddress)
            val proxyHandler = CordaRpcProxyHandler(executor, session, producer, consumer)
            log.info("Connected, returning proxy")
            @Suppress("UNCHECKED_CAST")
            return Proxy.newProxyInstance(rpcOpsClass.classLoader, arrayOf(rpcOpsClass), proxyHandler) as I
        }

        inline fun <reified I : RPCOps> connect(hostAndPort: HostAndPort, username: String, password: String): I {
            return connect(I::class.java, hostAndPort, username, password)
        }
    }
}
