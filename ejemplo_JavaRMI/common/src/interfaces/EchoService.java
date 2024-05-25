package interfaces;
import java.rmi.Remote;
import java.rmi.RemoteException;

// Interfaz remota del servicio de eco
public interface EchoService extends Remote {
    String echo (String s) throws RemoteException;
}
