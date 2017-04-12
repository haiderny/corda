package net.corda.node.services

import net.corda.core.contracts.DummyContract
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.contracts.TransactionType
import net.corda.core.crypto.Party
import net.corda.core.crypto.X509Utilities
import net.corda.core.crypto.commonName
import net.corda.core.div
import net.corda.core.getOrThrow
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.ServiceType
import net.corda.core.utilities.ALICE
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.flows.NotaryError
import net.corda.flows.NotaryException
import net.corda.flows.NotaryFlow
import net.corda.node.internal.AbstractNode
import net.corda.node.internal.Node
import net.corda.node.services.transactions.BFTNonValidatingNotaryService
import net.corda.node.utilities.ServiceIdentityGenerator
import net.corda.node.utilities.databaseTransaction
import net.corda.testing.node.NodeBasedTest
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.X500NameBuilder
import org.bouncycastle.asn1.x500.X500NameStyle
import org.junit.Test
import java.security.KeyPair
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BFTNotaryServiceTests : NodeBasedTest() {
    @Test
    fun `detect double spend`() {
        val notaryCommonName = "BFT Notary Service"
        val notaryNameRemainder = "O=R3,OU=corda,L=London,C=UK"
        val notaryLegalName = "CN=${notaryCommonName},${notaryNameRemainder}"
        val masterNode = startBFTNotaryCluster(notaryCommonName, notaryNameRemainder, 4, BFTNonValidatingNotaryService.type).first()
        val alice = startNode(ALICE.name).getOrThrow()

        val notaryParty = alice.netMapCache.getNotary(notaryLegalName)!!
        val notaryNodeKeyPair = databaseTransaction(masterNode.database) { masterNode.services.notaryIdentityKey }
        val aliceKey = databaseTransaction(alice.database) { alice.services.legalIdentityKey }

        val inputState = issueState(alice, notaryParty, notaryNodeKeyPair)

        val firstSpendTx = TransactionType.General.Builder(notaryParty).withItems(inputState).run {
            signWith(aliceKey)
            toSignedTransaction(false)
        }

        val firstSpend = alice.services.startFlow(NotaryFlow.Client(firstSpendTx))
        firstSpend.resultFuture.getOrThrow()

        val secondSpendTx = TransactionType.General.Builder(notaryParty).withItems(inputState).run {
            val dummyState = DummyContract.SingleOwnerState(0, alice.info.legalIdentity.owningKey)
            addOutputState(dummyState)
            signWith(aliceKey)
            toSignedTransaction(false)
        }
        val secondSpend = alice.services.startFlow(NotaryFlow.Client(secondSpendTx))

        val ex = assertFailsWith(NotaryException::class) { secondSpend.resultFuture.getOrThrow() }
        val error = ex.error as NotaryError.Conflict
        assertEquals(error.txId, secondSpendTx.id)
    }

    private fun issueState(node: AbstractNode, notary: Party, notaryKey: KeyPair): StateAndRef<*> {
        return databaseTransaction(node.database) {
            val tx = DummyContract.generateInitial(Random().nextInt(), notary, node.info.legalIdentity.ref(0))
            tx.signWith(node.services.legalIdentityKey)
            tx.signWith(notaryKey)
            val stx = tx.toSignedTransaction()
            node.services.recordTransactions(listOf(stx))
            StateAndRef(tx.outputStates().first(), StateRef(stx.id, 0))
        }
    }

    private fun startBFTNotaryCluster(notaryCommonName: String,
                                      notaryLegalNameRemainder: String,
                                      clusterSize: Int,
                                      serviceType: ServiceType): List<Node> {
        val quorum = (2 * clusterSize + 1) / 3
        val notaryName = "CN=$notaryCommonName,$notaryLegalNameRemainder"
        ServiceIdentityGenerator.generateToDisk(
                (0 until clusterSize).map { tempFolder.root.toPath() / "$notaryName-$it" },
                serviceType.id,
                notaryName,
                quorum)

        val serviceInfo = ServiceInfo(serviceType, notaryName.toString())
        val masterNode = startNode(
                "CN=$notaryCommonName-0,$notaryLegalNameRemainder",
                advertisedServices = setOf(serviceInfo),
                configOverrides = mapOf("notaryNodeId" to 0)
        ).getOrThrow()

        val remainingNodes = (1 until clusterSize).map {
            startNode(
                    "CN=$notaryCommonName-$it,$notaryLegalNameRemainder",
                    advertisedServices = setOf(serviceInfo),
                    configOverrides = mapOf("notaryNodeId" to it)
            ).getOrThrow()
        }

        return remainingNodes + masterNode
    }
}