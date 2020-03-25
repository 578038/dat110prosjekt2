package no.hvl.dat110.broker;

import java.util.Set;
import java.util.Collection;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher extends Stopable {

	private Storage storage;

	public Dispatcher(Storage storage) {
		super("Dispatcher");
		this.storage = storage;

	}

	@Override
	public void doProcess() {

		Collection<ClientSession> clients = storage.getSessions();

		Logger.lg(".");
		for (ClientSession client : clients) {

			Message msg = null;

			if (client.hasData()) {
				msg = client.receive();
			}

			// a message was received
			if (msg != null) {
				dispatch(client, msg);
			}
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void dispatch(ClientSession client, Message msg) {

		MessageType type = msg.getType();

		// invoke the appropriate handler method
		switch (type) {

		case DISCONNECT:
			onDisconnect((DisconnectMsg) msg);
			break;

		case CREATETOPIC:
			onCreateTopic((CreateTopicMsg) msg);
			break;

		case DELETETOPIC:
			onDeleteTopic((DeleteTopicMsg) msg);
			break;

		case SUBSCRIBE:
			onSubscribe((SubscribeMsg) msg);
			break;

		case UNSUBSCRIBE:
			onUnsubscribe((UnsubscribeMsg) msg);
			break;

		case PUBLISH:
			onPublish((PublishMsg) msg);
			break;

		default:
			Logger.log("broker dispatch - unhandled message type");
			break;

		}
	}

	// called from Broker after having established the underlying connection
	public void onConnect(ConnectMsg msg, Connection connection) {

		String user = msg.getUser();

		Logger.log("onConnect:" + msg.toString());

		storage.addClientSession(user, connection);
		//user just connected.
		//if any messages was sent while disconencted, they should be buffered
		// send any buffered message to user
		
	if(storage.getBufferedMessages(user) != null) {
	
		
		Set<Message> messages = storage.getBufferedMessages(user);
		for (Message message: messages) {
			storage.getSession(user).send(message);
		}
		// clear buffer
		storage.clearBufferedMessages(user);
	}
	
		
	}

	// called by dispatch upon receiving a disconnect message
	public void onDisconnect(DisconnectMsg msg) {

		String user = msg.getUser();

		Logger.log("onDisconnect:" + msg.toString());

		storage.removeClientSession(user);

	}

	public void onCreateTopic(CreateTopicMsg msg) {

		Logger.log("onCreateTopic:" + msg.toString());

		// TODO: create the topic in the broker storage
		// the topic is contained in the create topic message

		
		storage.createTopic(msg.getTopic());
		
		
		
		
		
		//throw new UnsupportedOperationException(TODO.method());

	}

	public void onDeleteTopic(DeleteTopicMsg msg) {

		Logger.log("onDeleteTopic:" + msg.toString());

		// TODO: delete the topic from the broker storage
		// the topic is contained in the delete topic message
		
		
		storage.deleteTopic(msg.getTopic());
		
		//throw new UnsupportedOperationException(TODO.method());
	}

	public void onSubscribe(SubscribeMsg msg) {

		Logger.log("onSubscribe:" + msg.toString());

		// TODO: subscribe user to the topic
		// user and topic is contained in the subscribe message
		
		String topic = msg.getTopic();
		String user = msg.getUser();
		
		storage.addSubscriber(user, topic);
		
		
		
		storage.addSubscriber(msg.getUser(), msg.getTopic());
		
	}

	public void onUnsubscribe(UnsubscribeMsg msg) {

		Logger.log("onUnsubscribe:" + msg.toString());

		// TODO: unsubscribe user to the topic
		// user and topic is contained in the unsubscribe message
		
		
		storage.removeSubscriber(msg.getUser(), msg.getTopic());
		
		
		//throw new UnsupportedOperationException(TODO.method());
	}

	public void onPublish(PublishMsg msg) {

		Logger.log("onPublish:" + msg.toString());

		// TODO: publish the message to clients subscribed to the topic
		// topic and message is contained in the subscribe message
		// messages must be sent used the corresponding client session objects
		
		//if client session is null, user is disconnected. Store message in bufferedMessages
		// instead of sending it storage.bufferMessage(user, msg)
		
		
		
		String topic = msg.getTopic();
		
		Set<String> subscribers = storage.getSubscribers(topic);
		
		Collection<ClientSession> clientsessions = storage.getSessions();
		
		
//		for(String user : subscribers) { //hvis bruker er i clients er han connected
//		if(clientsessions.contains(user)){
//			storage.getSession(user).send(msg);
//		}else {
//			storage.bufferMessage(user, msg);
//		}
			
		
		for(ClientSession clientsession : clientsessions) {
		if(storage.subscriptions.get(msg.getTopic()).contains(clientsession.getUser())) {
			
			clientsession.send(msg);
		}
		else {
			storage.bufferMessage(clientsession.getUser(), msg);
			}

		}
	}
}
