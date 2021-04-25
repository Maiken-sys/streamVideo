import java.io.*;
import java.net.Socket;

public class ActionsForClients implements Runnable{
    private final Socket clientSocket;

    public ActionsForClients(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }



    @Override
    public void run() {
        PrintWriter out = null;
        BufferedReader in = null;

        try{
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String line;
            while((line = in.readLine()) != null){
                System.out.printf("Sent from the client: %s\n", line);
                out.println(line);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally{
            try {
                if (out != null) {
                    out.close();
                } else if (in != null) {
                    in.close();
                    clientSocket.close();
                }
            }catch(IOException e1){
                    e1.printStackTrace();
                }

        }
    }
}
