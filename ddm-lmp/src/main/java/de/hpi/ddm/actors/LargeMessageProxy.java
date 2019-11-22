package de.hpi.ddm.actors;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import akka.NotUsed;
import akka.actor.*;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(BytesMessage.class, this::handle)
                .match(SourceMessage.class, this::handle)
                .match(SerializedByteMessage.class, this::deserializer)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        // Serializing the message
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Kryo kryo = new Kryo();
        Output output = new Output(byteArrayOutputStream);
        kryo.writeClassAndObject(output, message.getMessage());
        output.close();
        byte[] byteArrayData = byteArrayOutputStream.toByteArray();

        // Akka Streaming
        Source<List<Byte>, NotUsed> source = Source.from(Arrays.asList(ArrayUtils.toObject(byteArrayData))).grouped(262144);
        SourceRef<List<Byte>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.context().system());

        // Passing the source reference as a customized "SourceMessage"
        receiverProxy.tell(new SourceMessage(sourceRef, byteArrayData.length, this.sender(), message.getReceiver()), this.self());
    }

    private void handle(SourceMessage message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        // Receiving the customized "SourceMessage" and retrieving the source reference
        SourceRef<List<Byte>> sourceRef = message.getSourceRef();
        byte[] bytes = new byte[message.getLength()];
        sourceRef.getSource().runWith(Sink.seq(), this.context().system()).whenComplete
                (
                        (data, exception) -> {
                            int index = 0;
                            for (List<Byte> list : data) {
                                for (Byte abyte : list) {
                                    bytes[index] = abyte;
                                    index++;
                                }
                            }

                            SerializedByteMessage serializedByteMessage = new SerializedByteMessage(bytes, message.getReceiver(), message.getSender());
                            receiverProxy.tell(serializedByteMessage, message.getSender());
                        }
                );
    }

    private void handle(BytesMessage<?> message) {
        // Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        message.getReceiver().tell(message.getBytes(), message.getSender());
    }

    /////////////////
    // Actor State //
    /////////////////

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    private void deserializer(SerializedByteMessage<?> message) {
        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message.bytes);
            Kryo kryo = new Kryo();
            Input input = new Input(byteArrayInputStream);
            Object finalData = kryo.readClassAndObject(input);
            input.close();
            byteArrayInputStream.close();

            // Finally, we send the deserialize object to its destination
            message.receiver.tell(finalData, message.sender);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage<T> implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private T bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SerializedByteMessage<T> implements Serializable {
        private static final long serialVersionUID = 1234507743872319842L;
        private byte[] bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceMessage implements Serializable {
        private static final long serialVersionUID = 2432507743872319842L;
        private SourceRef<List<Byte>> sourceRef;
        private int length;
        private ActorRef sender;
        private ActorRef receiver;

    }

}
