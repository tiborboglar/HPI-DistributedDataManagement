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
    public static class SourceMessage implements Serializable {
        private static final long serialVersionUID = 3432507743872319842L;
        private SourceRef<List<Byte>> sourceRef;
        private int length;
        private ActorRef sender;
        private ActorRef receiver;

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

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(BytesMessage.class, this::handle)
                .match(SourceMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        try {
            // Serializing the data
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            Kryo kryo = new Kryo();
            Output output = new Output(byteArrayOutputStream);
            kryo.writeClassAndObject(output, message.getMessage());
            output.close();
            byte[] byteArrayData = byteArrayOutputStream.toByteArray();

            // Creating an Akka Streaming
            Source<List<Byte>, NotUsed> source = Source.from(Arrays.asList(ArrayUtils.toObject(byteArrayData))).grouped(20000);
            SourceRef<List<Byte>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.context().system());

            // Passing the source reference as a customized "SourceMessage"
            receiverProxy.tell(new SourceMessage(sourceRef, byteArrayData.length, this.sender(), message.getReceiver()), this.self());

        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private void handle(SourceMessage message) {
        // Receiving the customized "SourceMessage" that we created to retrieve our SourceRef
        message.getSourceRef()
                .getSource()
                .runWith(Sink.seq(), this.context().system())
                .whenComplete
                (
                    (data, exception) -> {
                        // Method to assemble the data, in this case we are creating an array of bytes
                        int i = 0;
                        byte[] bytes = new byte[message.getLength()];
                        for (List<Byte> list : data) {
                            for (Byte aByte : list) {
                                bytes[i] = aByte;
                                i++;
                            }
                        }

                        try {
                            // Deserialize the data using Kryo
                            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                            Kryo kryo = new Kryo();
                            Input input = new Input(byteArrayInputStream);
                            Object finalData = kryo.readClassAndObject(input);
                            input.close();
                            // Dispatch a message containing the deserialized data to the final destination
                            message.getReceiver().tell(finalData, message.getSender());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
    }


    private void handle(BytesMessage<?> message) {
        // Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        message.getReceiver().tell(message.getBytes(), message.getSender());
    }

}
