import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class AppNode{
    public PublisherImpl publisher;
    public ConsumerImpl consumer;


    public static void main(String [] Args){
        String host = "127.0.0.1";
        int port = 32000;

        try{
            Socket socket = new Socket(host,port);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            Scanner scanner = new Scanner(System.in);
            String line = null;
            while(!"exit".equalsIgnoreCase(line)){
                line = scanner.nextLine();
                out.println(line);
                System.out.println("Αυτό το μήνυμα στάλθηκε απο τον client χρησιμοποιώντας το PrintWriter out(socket.getOutputStream!)");
                out.flush();

            }
            scanner.close();
        } catch (UnknownHostException e){
            System.err.println("Προσπαθεις να συνδεθεις σε άγνωστο host!!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }
}
