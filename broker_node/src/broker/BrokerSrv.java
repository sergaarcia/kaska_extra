// Servidor que implementa la interfaz remota Kaska
package broker;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;
import java.nio.file.*;
import kaskaprot.KaskaSrv;
import kaskaprot.TopicWithOffset;

class BrokerSrv extends UnicastRemoteObject implements KaskaSrv {
	public static final long serialVersionUID = 1234567890L;
	private static final byte[] MAGIC_NUMBER = "KASK".getBytes();

	private Map<String, List<byte[]>> topics;
	private Map<String, Map<String, Integer>> clientOffsets;
	private File commitsDir;
	private File dataDir;

	public BrokerSrv() throws RemoteException {
		topics = new ConcurrentHashMap<>();
		clientOffsets = new ConcurrentHashMap<>();
	}

	public BrokerSrv(String directory) throws RemoteException {
		topics = new ConcurrentHashMap<>();
		clientOffsets = new ConcurrentHashMap<>();
		dataDir = new File(directory, "data");
		commitsDir = new File(directory, "commits");
		if (!dataDir.exists()) {
			dataDir.mkdirs();
		}
		if (!commitsDir.exists()) {
			commitsDir.mkdirs();
		}
		recoverState();
	}

	// se crean temas devolviendo cuántos se han creado
	public synchronized int createTopics(Collection<String> topics) throws RemoteException {
		int count = 0;
		for (String topic : topics) {
			if (!this.topics.containsKey(topic)) {
				this.topics.put(topic, new ArrayList<>());
				try {
					writeMessagesToFile(topic, new ArrayList<>());
					count++;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return count;
	}

	// devuelve qué temas existen
	public synchronized Collection<String> topicList() throws RemoteException {
		return new ArrayList<>(topics.keySet());
	}

	// envía un array bytes devolviendo error si el tema no existe
	public synchronized boolean send(String topic, byte[] m) throws RemoteException {
		List<byte[]> topicMessages = topics.get(topic);
		if (topicMessages == null) {
			return false;
		}
		topicMessages.add(m);
		try {
			writeMessagesToFile(topic, topicMessages);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// lee un determinado mensaje de un tema devolviendo null si error
	// (tema no existe o mensaje no existe)
	// se trata de una función para probar el buen funcionamiento de send;
	// en Kafka los mensajes se leen con poll
	public synchronized byte[] get(String topic, int offset) throws RemoteException {
		List<byte[]> topicMessages = topics.get(topic);
		if (topicMessages == null || offset >= topicMessages.size()) {
			return null;
		}
		return topicMessages.get(offset);
	}

	// obtiene el offset actual de estos temas en el broker ignorando
	// los temas que no existen
	public synchronized Collection<TopicWithOffset> endOffsets(Collection<String> topics) throws RemoteException {
		List<TopicWithOffset> endOffsets = new ArrayList<>();
		for (String topic : topics) {
			List<byte[]> topicMessages = this.topics.get(topic);
			if (topicMessages != null) {
				endOffsets.add(new TopicWithOffset(topic, topicMessages.size()));
			}
		}
		return endOffsets;
	}

	// obtiene todos los mensajes no leídos de los temas suscritos
	public synchronized Map<String, List<byte[]>> poll(Collection<TopicWithOffset> topics) throws RemoteException {
		Map<String, List<byte[]>> unreadMessages = new HashMap<>();
		for (TopicWithOffset topicWithOffset : topics) {
			String topic = topicWithOffset.getTopic();
			int offset = topicWithOffset.getOffset();
			List<byte[]> topicMessages = this.topics.get(topic);
			if (topicMessages != null) {
				List<byte[]> unread = topicMessages.subList(offset, topicMessages.size());
				unreadMessages.put(topic, new ArrayList<>(unread));
			}
		}
		return unreadMessages;
	}

	// salva los offsets especificados devolviendo cuántos se han guardado
	public synchronized int commit(String client, Collection<TopicWithOffset> topics) throws RemoteException {
		clientOffsets.putIfAbsent(client, new HashMap<>());
		Map<String, Integer> offsets = clientOffsets.get(client);
		int count = 0;
		for (TopicWithOffset topicWithOffset : topics) {
			String topic = topicWithOffset.getTopic();
			if (this.topics.containsKey(topic)) {
				offsets.put(topic, topicWithOffset.getOffset());
				saveOffset(client, topic, topicWithOffset.getOffset());
				count++;
			}
		}
		return count;
	}

	// recupera offsets guardados correspondientes a los temas especificados
	public synchronized Collection<TopicWithOffset> committed(String client, Collection<String> topics)
			throws RemoteException {
		Map<String, Integer> offsets = clientOffsets.get(client);
		List<TopicWithOffset> result = new ArrayList<>();
		if (offsets != null) {
			for (String topic : topics) {
				Integer offset = offsets.get(topic);
				if (offset != null) {
					result.add(new TopicWithOffset(topic, offset));
				} else {
					offset = loadOffset(client, topic);
					if (offset != null) {
						offsets.put(topic, offset);
						result.add(new TopicWithOffset(topic, offset));
					}
				}
			}
		}
		return result;
	}

	private void saveOffset(String client, String topic, int offset) {
		File clientDir = new File(commitsDir, client);
		if (!clientDir.exists()) {
			clientDir.mkdirs();
		}
		File topicFile = new File(clientDir, topic);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(topicFile))) {
			writer.write(String.valueOf(offset));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Integer loadOffset(String client, String topic) {
		File clientDir = new File(commitsDir, client);
		File topicFile = new File(clientDir, topic);
		if (topicFile.exists()) {
			try (BufferedReader reader = new BufferedReader(new FileReader(topicFile))) {
				return Integer.parseInt(reader.readLine());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private void recoverState() {
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir.toPath())) {
			for (Path entry : stream) {
				if (Files.isRegularFile(entry) && isKaskaFile(entry)) {
					String topic = entry.getFileName().toString();
					List<byte[]> messages = readMessagesFromFile(entry);
					topics.put(topic, messages);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean isKaskaFile(Path file) throws IOException {
		try (InputStream is = Files.newInputStream(file)) {
			byte[] magic = new byte[MAGIC_NUMBER.length];
			if (is.read(magic) != MAGIC_NUMBER.length) {
				return false;
			}
			return Arrays.equals(magic, MAGIC_NUMBER);
		}
	}

	private List<byte[]> readMessagesFromFile(Path file) throws IOException {
		List<byte[]> messages = new ArrayList<>();
		try (InputStream is = Files.newInputStream(file); DataInputStream dis = new DataInputStream(is)) {
			dis.skipBytes(MAGIC_NUMBER.length); // Skip the magic number
			while (dis.available() > 0) {
				int length = dis.readInt();
				byte[] message = new byte[length];
				dis.readFully(message);
				messages.add(message);
			}
		}
		return messages;
	}

	private void writeMessagesToFile(String topic, List<byte[]> messages) throws IOException {
		Path filePath = dataDir.toPath().resolve(topic);
		try (OutputStream os = Files.newOutputStream(filePath, StandardOpenOption.CREATE);
				DataOutputStream dos = new DataOutputStream(os)) {
			dos.write(MAGIC_NUMBER);
			for (byte[] message : messages) {
				dos.writeInt(message.length);
				dos.write(message);
			}
		}
	}

	static public void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Usage: BrokerSrv registryPortNumber directory");
			return;
		}
		try {
			BrokerSrv srv = new BrokerSrv(args[1]);
			Registry registry = LocateRegistry.getRegistry(Integer.parseInt(args[0]));
			registry.rebind("Kaska", srv);
		} catch (Exception e) {
			System.err.println("Broker exception: " + e.toString());
			System.exit(1);
		}
	}
}
