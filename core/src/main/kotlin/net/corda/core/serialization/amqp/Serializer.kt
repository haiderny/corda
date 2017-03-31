package net.corda.core.serialization.amqp

import org.apache.qpid.proton.codec.Data
import java.lang.reflect.Type

abstract class Serializer {
    abstract val typeDescriptor: String
    abstract val type: Type
    abstract fun writeClassInfo(output: SerializationOutput)
    abstract fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput)
    abstract fun readObject(obj: Any, envelope: Envelope, input: DeserializationInput): Any
}