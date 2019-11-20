package de.hpi.ddm.actors;

import java.io.*;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import de.hpi.ddm.structures.Chunker;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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

		public T getMessage() {
			return message;
		}

		public ActorRef getReceiver() {
			return receiver;
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
		private Integer serializerID;
		private String manifest;

		public T getBytes() {
			return bytes;
		}

		public ActorRef getReceiver() {
			return receiver;
		}

		public ActorRef getSender() {
			return sender;
		}

		public Integer getSerializerID() {
			return serializerID;
		}

		public String getManifest() {
			return manifest;
		}
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
				.match(ByteString.class, this::handle)
				.matchEquals("complete", completed ->{
					System.out.println("ByteString sent successfully.");
				})
				.matchAny(object -> {
                    System.out.println(object.getClass());
                    this.log().info("Received unknown message: \"{}\"", object.toString());
                })
				.build();
	}

	private void handle(ByteString byteString) throws Exception {
		System.out.println("Got message from sender, and the message has length " + byteString.length());

		// we have to retrieve the manifest and the serializer
		byte[] byteArrayMessage = byteString.toArray();
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayMessage);

	}

	private void handle(LargeMessage<?> message) throws IOException {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		Serialization serialization = SerializationExtension.get(getContext().getSystem());
		byte[] bytes = serialization.serialize(message).get();
		int serializerId = serialization.findSerializerFor(message).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message), message);

		BytesMessage bytesMessage = new BytesMessage(bytes, this.sender(), message.getReceiver(), serializerId, manifest);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(bytesMessage);
			objectOutputStream.flush();
			objectOutputStream.close();
			byteArrayOutputStream.close();
		} catch (IOException ex) {
			System.out.println("IOException is caught");
		}

		byte[] byteArrayData = byteArrayOutputStream.toByteArray();
		ByteString byteString = ByteString.fromArray(byteArrayData);

		CompletionStage<ActorRef> resolveOne = receiverProxy.resolveOne(java.time.Duration.ofSeconds(30));
		ActorRef finalReceiver = null;
		try {
			finalReceiver = resolveOne.toCompletableFuture().get();
		} catch (Exception ex) {
			System.out.println("Error in getting actor");
		}

		Materializer materializer = ActorMaterializer.create(getContext().getSystem());

		Source<ByteString, NotUsed> source = Source.single(byteString);
		Source<ByteString, NotUsed> chunkSource = source.via(new Chunker(262144));
		Sink<ByteString, NotUsed> sink = Sink.actorRef(getContext().getSelf(), "complete");
		chunkSource.map(x -> x).runWith(sink, materializer);

		System.out.println("ByteString is sent!");
	}

	private void handle(BytesMessage<?> message) {

		System.out.println("BytesMessage is received!");
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
