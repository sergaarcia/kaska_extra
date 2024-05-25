// Clase de cliente que proporciona los métodos para interacciona con el broker.
// Corresponde al API ofrecida a las aplicaciones 
package kaska;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.util.Collection;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.function.Function;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import kaskaprot.KaskaSrv;
import kaskaprot.TopicWithOffset;

public class KaskaClient {
	private KaskaSrv kaskaSrv;
	private Map<String, TopicWithOffset> subscriptions;
	private String client;

	// constructor: realiza el lookup del servicio en el Registry
	public KaskaClient(String host, String port, String client) throws RemoteException, NotBoundException {
		Registry registry = LocateRegistry.getRegistry(host, Integer.parseInt(port));
		kaskaSrv = (KaskaSrv) registry.lookup("Kaska");
		this.client = client;
	}

	// se crean temas devolviendo cuántos se han creado
	public int createTopics(Collection<String> topics) throws RemoteException {
		// primero elimino duplicados
		HashSet<String> topicSet = new HashSet<>(topics);
		return kaskaSrv.createTopics(topicSet);
	}

	// función de conveniencia para crear un solo tema
	public boolean createOneTopic(String topic) throws RemoteException {
		return createTopics(Arrays.asList(topic)) == 1;
	}

	// devuelve qué temas existen
	public Collection<String> topicList() throws RemoteException {
		return kaskaSrv.topicList();
	}

	// envía un mensaje devolviendo error si el tema no existe o el
	// objeto no es serializable
	public boolean send(String topic, Object o) throws RemoteException {
		byte[] serializedMessage = serializeMessage(o);
		if (serializedMessage == null) {
			return false;
		}
		return kaskaSrv.send(topic, serializedMessage);
	}

	// lee mensaje pedido de un tema devolviéndolo en un Record
	// (null si error: tema no existe o mensaje no existe);
	// se trata de una función para probar el buen funcionamiento de send;
	// en Kafka los mensajes se leen con poll
	public Record get(String topic, int offset) throws RemoteException {
		byte[] message = kaskaSrv.get(topic, offset);
		if (message == null) {
			return null;
		}
		return new Record(topic, offset, message);
	}

	// se suscribe a los temas pedidos devolviendo a cuántos se ha suscrito
	public int subscribe(Collection<String> topics) throws RemoteException {
		// primero elimino duplicados
		HashSet<String> topicSet = new HashSet<>(topics);
		Collection<TopicWithOffset> endOffsets = kaskaSrv.endOffsets(topicSet);
		subscriptions = endOffsets.stream().collect(Collectors.toMap(TopicWithOffset::getTopic, Function.identity()));
		return subscriptions.size();
	}

	// función de conveniencia para suscribirse a un solo tema
	public boolean subscribeOneTopic(String topic) throws RemoteException {
		return subscribe(Arrays.asList(topic)) == 1;
	}

	// se da de baja de todas las suscripciones
	public void unsubscribe() {
		subscriptions.clear();
	}

	// obtiene el offset local de un tema devolviendo -1 si no está suscrito a ese
	// tema
	public int position(String topic) {
		TopicWithOffset topicWithOffset = subscriptions.get(topic);
		if (topicWithOffset == null) {
			return -1;
		}
		return topicWithOffset.getOffset();
	}

	// modifica el offset local de un tema devolviendo error si no está suscrito a
	// ese tema
	public boolean seek(String topic, int offset) {
		TopicWithOffset topicWithOffset = subscriptions.get(topic);
		if (topicWithOffset == null) {
			return false;
		}
		topicWithOffset.setOffset(offset);
		return true;
	}

	// obtiene todos los mensajes no leídos de los temas suscritos
	// devuelve null si no está suscrito a ningún tema
	public List<Record> poll() throws RemoteException {
		if (subscriptions.isEmpty()) {
			return null;
		}
		Collection<TopicWithOffset> topics = new ArrayList<>(subscriptions.values());
		Map<String, List<byte[]>> messages = kaskaSrv.poll(topics);
		List<Record> records = new ArrayList<>();
		for (Map.Entry<String, List<byte[]>> entry : messages.entrySet()) {
			String topic = entry.getKey();
			List<byte[]> topicMessages = entry.getValue();
			for (int i = 0; i < topicMessages.size(); i++) {
				byte[] message = topicMessages.get(i);
				int currentOffset = subscriptions.get(topic).getOffset();
				records.add(new Record(topic, currentOffset, message));
				subscriptions.get(topic).setOffset(currentOffset + 1);
			}
		}
		return records;
	}

	// salva los offsets actuales de los temas suscritos por el cliente
	public boolean commit() throws RemoteException {
		Collection<TopicWithOffset> topics = new ArrayList<>(subscriptions.values());
		return kaskaSrv.commit(client, topics) == topics.size();
	}

	// salva el offset del tema especificado por el cliente
	public boolean commit(String topic, int offset) throws RemoteException {
		TopicWithOffset topicWithOffset = subscriptions.get(topic);
		if (topicWithOffset == null) {
			return false;
		}
		topicWithOffset.setOffset(offset);
		return kaskaSrv.commit(client, Arrays.asList(topicWithOffset)) == 1;
	}

	// actualiza offsets con los guardados para los temas suscritos
	public boolean committed() throws RemoteException {
		Collection<String> topics = new ArrayList<>(subscriptions.keySet());
		Collection<TopicWithOffset> committedOffsets = kaskaSrv.committed(client, topics);
		for (TopicWithOffset offset : committedOffsets) {
			subscriptions.put(offset.getTopic(), offset);
		}
		return committedOffsets.size() == topics.size();
	}

	// actualiza offset del tema especificado con el guardado
	public boolean committed(String topic) throws RemoteException {
		Collection<String> topics = new ArrayList<>(Arrays.asList(topic));
		Collection<TopicWithOffset> committedOffsets = kaskaSrv.committed(client, Arrays.asList(topic));
		if (committedOffsets.isEmpty()) {
			return false;
		}
		subscriptions.put(topic, committedOffsets.iterator().next());
		return true;
	}

	// función interna que serializa un objeto en un array de bytes
	// devuelve null si el objeto no es serializable
	byte[] serializeMessage(Object o) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(o);
			return bos.toByteArray();
		} catch (IOException e) {
			System.err.println("Error en la serialización: " + e.toString());
			return null;
		}
	}
}