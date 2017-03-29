package net.corda.client.rpc

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.pool.KryoCallback
import com.esotericsoftware.kryo.pool.KryoPool
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import kotlinx.support.jdk8.collections.parallelStream
import net.corda.core.*
import net.corda.core.messaging.RPCOps
import net.corda.core.serialization.KryoPoolWithContext
import net.corda.core.utilities.loggerFor
import net.corda.node.driver.*
import net.corda.nodeapi.*
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.*
import org.apache.activemq.artemis.core.config.Configuration
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory
import org.apache.activemq.artemis.core.security.CheckType
import org.apache.activemq.artemis.core.security.Role
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager
import rx.Observable
import rx.Subscriber
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


data class RpcServerHandle<I : RPCOps>(
        val hostAndPort: HostAndPort
)

interface RpcExposedDSLInterface : DriverDSLExposedInterface {
    fun <I : RPCOps> startRpcServer(
            name: String,
            ops : I,
            authenticate: (user: String?, password: String?) -> Boolean = { user, password -> true }
    ) : ListenableFuture<RpcServerHandle<I>>
}

interface RpcInternalDSLInterface : DriverDSLInternalInterface, RpcExposedDSLInterface

fun <A> rpcDriver(
        isDebug: Boolean = false,
        driverDirectory: Path = Paths.get("build", getTimestampAsDirectoryName()),
        portAllocation: PortAllocation = PortAllocation.Incremental(10000),
        sshdPortAllocation: PortAllocation = PortAllocation.Incremental(20000),
        debugPortAllocation: PortAllocation = PortAllocation.Incremental(5005),
        systemProperties: Map<String, String> = emptyMap(),
        useTestClock: Boolean = false,
        automaticallyStartNetworkMap: Boolean = true,
        dsl: RpcExposedDSLInterface.() -> A
) = genericDriver(
        driverDsl = RpcDriverDSL(
                DriverDSL(
                        portAllocation = portAllocation,
                        sshdPortAllocation = sshdPortAllocation,
                        debugPortAllocation = debugPortAllocation,
                        systemProperties = systemProperties,
                        driverDirectory = driverDirectory.toAbsolutePath(),
                        useTestClock = useTestClock,
                        automaticallyStartNetworkMap = automaticallyStartNetworkMap,
                        isDebug = isDebug
                )
        ),
        coerce = { it },
        dsl = dsl
)

data class RpcDriverDSL(
        val driverDSL: DriverDSL
) : DriverDSLInternalInterface by driverDSL, RpcInternalDSLInterface {

    companion object {
        private fun createRpcServerArtemisConfig(baseDirectory: Path, hostAndPort: HostAndPort): Configuration {
            val connectionDirection = ConnectionDirection.Inbound(acceptorFactoryClassName = NettyAcceptorFactory::class.java.name)
            return ConfigurationImpl().apply {
                val artemisDir = "$baseDirectory/artemis"
                bindingsDirectory = "$artemisDir/bindings"
                journalDirectory = "$artemisDir/journal"
                largeMessagesDirectory = "$artemisDir/large-messages"
                acceptorConfigurations = setOf(ArtemisTcpTransport.tcpTransport(connectionDirection, hostAndPort, null))
                queueConfigurations = listOf(
                        CoreQueueConfiguration().apply {
                            name = RpcApi.RPC_SERVER_QUEUE_NAME
                            address = RpcApi.RPC_SERVER_QUEUE_NAME
                            isDurable = false
                        }
                )
            }
        }
    }

    override fun <I : RPCOps> startRpcServer(
            name: String,
            ops: I,
            authenticate: (user: String?, password: String?) -> Boolean
    ): ListenableFuture<RpcServerHandle<I>> {
        val hostAndPort = driverDSL.portAllocation.nextHostAndPort()
        return driverDSL.executorService.submit<RpcServerHandle<I>> {
            val baseDir = driverDSL.driverDirectory / name
            val artemisConfig = createRpcServerArtemisConfig(baseDir, hostAndPort)
            val securityManager = object : ActiveMQSecurityManager {
                override fun validateUser(user: String?, password: String?) = authenticate(user, password)
                override fun validateUserAndRole(user: String?, password: String?, roles: MutableSet<Role>?, checkType: CheckType?) = authenticate(user, password)
            }

            val server = ActiveMQServerImpl(artemisConfig, securityManager)
            server.start()
            driverDSL.shutdownManager.registerShutdown(Futures.immediateFuture {
                server.stop()
            })
            val locator = ActiveMQClient.createServerLocatorWithoutHA()
            val transport = ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), hostAndPort, null)
            val sessionFactory = locator.createSessionFactory(transport)
            val session = sessionFactory.createSession()
            driverDSL.shutdownManager.registerShutdown(Futures.immediateFuture {
                session.stop()
                sessionFactory.close()
            })

            val consumer = session.createConsumer(RpcApi.RPC_SERVER_QUEUE_NAME)
            val producer = session.createProducer()

            val rpcServer = CordaRpcServer(ops, driverDSL.executorService, producer, session, consumer)
            consumer.setMessageHandler(rpcServer::messageHandler)
            session.start()

            RpcServerHandle(hostAndPort)
        }
    }

}

class CordaRpcServer<I : RPCOps>(
        private val ops: I,
        private val executor: ExecutorService,
        private val producer: ClientProducer,
        session: ClientSession,
        consumer: ClientConsumer
) {
    private val artemisState = ThreadBox(object {
        val session = session
        val consumer = consumer
    })

    private val methodTable = ops.javaClass.declaredMethods.groupBy { it.name }.mapValues { it.value.single() }

    // We create a separate single thread executor for the producer. This is so that we avoid deadlocking in cases where
    // during serialisation we encounter an Observable that already has some observations. In this case the observation
    // serialisation will just be queued up to this executor.
    private val producerExecutor = Executors.newSingleThreadExecutor()

    private interface ObservableContext {
        fun sendMessage(serverToClient: RpcApi.ServerToClient)
    }

    companion object {
        private val log = loggerFor<CordaRpcServer<*>>()
        private val observableSerializer = object : Serializer<Observable<Any>>() {
            override fun read(kryo: Kryo?, input: Input?, type: Class<Observable<Any>>?): Observable<Any> {
                TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
            }

            override fun write(kryo: Kryo, output: Output, observable: Observable<Any>) {
                val observableId = RpcApi.ObservableId(random63BitValue())
                val observableContext = kryo.context[RpcObservableContextKey] as ObservableContext
                output.writeLong(observableId.toLong, true)
                observable.subscribe(
                        object : Subscriber<Any>() {
                            override fun onNext(observation: Any) {
                                observableContext.sendMessage(RpcApi.ServerToClient.Observation(observableId, observation))
                            }
                            override fun onError(exception: Throwable) {
                                observableContext.sendMessage(RpcApi.ServerToClient.ObservableClosed(observableId, exception))
                            }
                            override fun onCompleted() {
                                observableContext.sendMessage(RpcApi.ServerToClient.ObservableClosed(observableId, null))
                            }
                        }
                )
            }
        }
        private object RpcObservableContextKey
        private val kryoPool = KryoPool.Builder { RPCKryo(observableSerializer) }.build()
        private fun createKryoPoolWithObservableContext(observableContext: ObservableContext): KryoPool {
            return KryoPoolWithContext(kryoPool, RpcObservableContextKey, observableContext)
        }
    }

    fun messageHandler(artemisMessage: ClientMessage) {
        val clientToServer = RpcApi.ClientToServer.fromClientMessage(executor, kryoPool, artemisMessage)
        log.info("Got message from RPC client $clientToServer")
        clientToServer.accept(
                onRpcRequest = { rpcRequest ->
                    val result = ErrorOr.catch {
                        val method = methodTable[rpcRequest.methodName] ?: throw RPCException("Received RPC for unknown method ${rpcRequest.methodName} - possible client/server version skew?")
                        method.invoke(ops, *rpcRequest.arguments.toTypedArray())
                    }
                    val reply = RpcApi.ServerToClient.RpcReply(
                            id = rpcRequest.id,
                            result = result
                    )
                    val observableContext = object : ObservableContext {
                        val kryoPoolWithObservableContext = createKryoPoolWithObservableContext(this)
                        override fun sendMessage(serverToClient: RpcApi.ServerToClient) {
                            val message = artemisState.locked { session.createMessage(false) }
                            serverToClient.writeToClientMessage(executor, kryoPoolWithObservableContext, message)
                            producerExecutor.submit {
                                producer.send(rpcRequest.clientAddress, message)
                            }
                        }
                    }
                    observableContext.sendMessage(reply)
                },
                onObservableClosed = {
                    TODO("ASD")
                }
        )

    }
}

interface Hello : RPCOps {
    fun echo(string: String): String
    fun delayedEcho(string: String): Array<ListenableFuture<String>>
}

fun main(args: Array<String>) {
    rpcDriver(automaticallyStartNetworkMap = false) {
        val handleFuture = startRpcServer("Alice", object : Hello {
            override val protocolVersion = 0
            override fun echo(string: String) = string
            override fun delayedEcho(string: String) = Array(100) { future { Thread.sleep(100L) ; "$it" } }
        })

        val hello = CordaRPCClient2.connect<Hello>(handleFuture.get().hostAndPort, "", "")
        val asd = hello.delayedEcho("bello")
        println("${Futures.allAsList(asd.toList()).get()}")
        Thread.sleep(1000)
//        (1..1000).toList().parallelStream().forEach {
//            println(hello.echo("$it"))
//        }
    }
}