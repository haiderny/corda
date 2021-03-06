package net.corda.traderdemo

import com.google.common.util.concurrent.Futures
import net.corda.client.rpc.CordaRPCClient
import net.corda.core.getOrThrow
import net.corda.core.node.services.ServiceInfo
import net.corda.core.utilities.DUMMY_BANK_A
import net.corda.core.utilities.DUMMY_BANK_B
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.flows.IssuerFlow
import net.corda.node.services.startFlowPermission
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.nodeapi.User
import net.corda.testing.BOC
import net.corda.testing.node.NodeBasedTest
import org.junit.Test

class TraderDemoTest : NodeBasedTest() {
    @Test
    fun `runs trader demo`() {
        val permissions = setOf(
                startFlowPermission<IssuerFlow.IssuanceRequester>(),
                startFlowPermission<net.corda.traderdemo.flow.SellerFlow>())
        val demoUser = listOf(User("demo", "demo", permissions))
        val user = User("user1", "test", permissions = setOf(startFlowPermission<IssuerFlow.IssuanceRequester>()))
        val (nodeA, nodeB) = Futures.allAsList(
                startNode(DUMMY_BANK_A.name, rpcUsers = demoUser),
                startNode(DUMMY_BANK_B.name, rpcUsers = demoUser),
                startNode(BOC.name, rpcUsers = listOf(user)),
                startNode(DUMMY_NOTARY.name, setOf(ServiceInfo(SimpleNotaryService.type)))
        ).getOrThrow()

        val (nodeARpc, nodeBRpc) = listOf(nodeA, nodeB).map {
            val client = CordaRPCClient(it.configuration.rpcAddress!!)
            client.start(demoUser[0].username, demoUser[0].password).proxy()
        }

        TraderDemoClientApi(nodeARpc).runBuyer()
        TraderDemoClientApi(nodeBRpc).runSeller(counterparty = nodeA.info.legalIdentity.name)
    }
}
