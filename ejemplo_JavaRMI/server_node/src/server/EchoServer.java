package server;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import interfaces.EchoService;

// Objeto remoto que actúa de servidor de eco
class EchoServer extends UnicastRemoteObject implements EchoService  {
    public static final long serialVersionUID=1234567890L;
    EchoServer() throws RemoteException { }
    // funcionalidad del servidor
    public String echo(String s) throws RemoteException {
        return s.toUpperCase();
    }
    static public void main (String args[]) {
       if (args.length!=1) {
            System.err.println("Usage: EchoServer RegistryPort"); return;
        }
        try {
            // instancia un objeto de esta clase
            EchoServer srv = new EchoServer();
            // localiza el registry en el puerto especificado de esta máquina
            Registry registry = LocateRegistry.getRegistry(Integer.parseInt(args[0]));
            // da de alta el servicio
            registry.rebind("Echo", srv);
        }
        catch (Exception e) {
            System.err.println("EchoServer exception:");
            e.printStackTrace(); System.exit(1);
        }
    }
}
