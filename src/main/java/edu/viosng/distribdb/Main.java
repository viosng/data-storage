import org.apache.thrift.transport.TTransportException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: vio
 * Date: 29.11.12
 */
public class Main {
    public static void genFiles(int count, int commandsCount){
        try {
            FileWriter out;
            Random random = new Random();
            random.setSeed(System.currentTimeMillis());
            File myPath = new File("Files/");
            myPath.mkdirs();
            for(int i = 0; i < count; i++){
                out = new FileWriter("Files/File" + i + ".txt");
                int maxId = count * commandsCount;
                for(int j = 0; j < commandsCount; j++){
                    int com = random.nextInt(4);
                    switch (com){
                        case 0:
                            out.write(String.format("add %d %d\n", random.nextInt(count), random.nextInt(commandsCount)));
                            break;
                        case 1:
                            out.write(String.format("update %d %s %d\n", random.nextInt(maxId),
                                    random.nextInt(2) % 2 == 1 ? "name" : "phone", random.nextInt(maxId)));
                            break;
                        case 2:
                            out.write(String.format("del %d\n", random.nextInt(maxId)));
                            break;
                        case 3:
                            out.write(String.format("read %s\n", random.nextInt(150) == 0
                                    ? "*" : Integer.toString(random.nextInt(maxId))));
                            break;
                    }
                }
                out.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void testConcurrent(int clientsCount, int commandsCount){
        genFiles(clientsCount, commandsCount);
        TestDB test = null;
        TestDB.count = clientsCount;
        try {
            test = new TestDB(true);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        test.test();
    }
    public static void testAdminCommandLine(){
        Shard shard1 = new Shard(8081, 3);
        Shard shard2 = new Shard(8085, 2);
        Shard shard3 = new Shard(8083, 1);
        Balancer balancer = new Balancer("conf");
        AdminCommandLine admin = null;
        try {
            admin = new AdminCommandLine(balancer);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        admin.start(new BufferedReader(new InputStreamReader(System.in)));
    }
    public static void main(String args[]){
        //testConcurrent(10, 20);
        testAdminCommandLine();
    }
}
