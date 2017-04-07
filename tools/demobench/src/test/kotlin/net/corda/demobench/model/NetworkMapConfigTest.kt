package net.corda.demobench.model

import net.corda.core.crypto.X509Utilities
import org.junit.Test
import kotlin.test.assertEquals

class NetworkMapConfigTest {

    @Test
    fun keyValue() {
        val config = NetworkMapConfig(X509Utilities.getDevX509Name("My\tNasty Little\rLabel\n"), 10000)
        assertEquals("mynastylittlelabel", config.key)
    }

    @Test
    fun removeWhitespace() {
        assertEquals("OneTwoThreeFour!", "One\tTwo   \rThree\r\nFour!".stripWhitespace())
    }

}
