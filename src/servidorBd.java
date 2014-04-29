import java.io.*;
import java.net.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class servidorBd {
    static Connection connection;
    static void conectar(String baseDatos) {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3307/"+baseDatos,"root","qwerty");
            System.out.println("Conexión correcta");
        }catch (SQLException e){
            e.printStackTrace();
            return;
        }
    }
    public static void main(String args[]) throws Exception {
        String clientSentence;
        String capitalizedSentence = "";
        ServerSocket welcomeSocket = new ServerSocket(6789);
        PreparedStatement pstatement = null;
        while(true) {
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            clientSentence = inFromClient.readLine();
            String[] cachos = clientSentence.split("::");
            conectar(cachos[1]);
            if (cachos[0].equals("List")) {
                try {
                    pstatement = connection.prepareStatement("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA= ?");
                    pstatement.setString(1, cachos[1]);
                    ResultSet rs = pstatement.executeQuery();
                    while (rs.next()){
                        capitalizedSentence += rs.getString(1) +"; ";
                    }
                    rs.close();
                }catch (SQLException e){
                    e.printStackTrace();
                    return;
                }
                capitalizedSentence += '\n';
                outToClient.writeBytes(capitalizedSentence);
            } 
            if (cachos[0].equals("Num")) {
                try {
                    pstatement = connection.prepareStatement("SELECT COUNT(*) AS total FROM "+cachos[1]+'.'+cachos[2]);
                    ResultSet rs = pstatement.executeQuery();
                    if (rs.next()){
                        capitalizedSentence = rs.getString("total");
                    }
                    rs.close();
                }catch (SQLException e){
                    e.printStackTrace();
                    return;
                }
                capitalizedSentence += '\n';
                outToClient.writeBytes(capitalizedSentence);
            }
            if (cachos[0].equals("Del")) {
                try {
                    pstatement = connection.prepareStatement("DELETE FROM "+cachos[1]+'.'+cachos[2]);
                    int resultado = pstatement.executeUpdate();
                    if (resultado == 1){
                        capitalizedSentence = "Non se puido eliminar a taboa "+cachos[2];
                    } else {
                        capitalizedSentence = "Taboa eliminada correctamente";
                    }
                    
                }catch (SQLException e){
                    e.printStackTrace();
                    return;
                }
                
                outToClient.writeBytes(capitalizedSentence + '\n');
            }
            if (cachos[0].equals("Ren")) {
                try {
                    pstatement = connection.prepareStatement("RENAME TABLE "+cachos[1]+'.'+cachos[2]+ " TO " + cachos[1]+'.'+cachos[3]);
                    int resultado = pstatement.executeUpdate();
                    if (resultado == 1){
                        capitalizedSentence = "Non se puido cambiar de nome a taboa "+cachos[2];
                    } else {
                        capitalizedSentence = "Nome de taboa cambiado correctamente";
                    }
                }catch (SQLException e){
                    e.printStackTrace();
                    return;
                }
                
                outToClient.writeBytes(capitalizedSentence + '\n');
            }
             
        }
    }
}
