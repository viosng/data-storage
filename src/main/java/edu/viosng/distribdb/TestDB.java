import org.apache.thrift.transport.TTransportException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class TestDB {
    private Balancer balancer;
    public static int count = 10;
    private ArrayList<CommandLineClient> clients;
    public static boolean testConcurrent = false;
    public TestDB(boolean test) throws TTransportException {
        testConcurrent = test;
        count = (testConcurrent ? count : 1);
        Shard shard1 = new Shard(8081, 3);
        Shard shard2 = new Shard(8085, 3);
        Shard shard3 = new Shard(8083, 3);
        balancer = new Balancer("conf");
        clients = new ArrayList<CommandLineClient>();
        for(int i = 0; i < (testConcurrent ? count : 1); i++){
            CommandLineClient client = new CommandLineClient(8080, i);
            client.number = i;
            clients.add(client);
        }
    }
    public void test(){
        try{
            long time1 = System.currentTimeMillis();
            int n = count;
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for(int i = 1; i < n; i++){
                Thread t = new Thread(clients.get(i));
                threads.add(t);
                t.start();
            }
            if(testConcurrent){
                clients.get(0).run();
            } else {
                clients.get(0).start(new BufferedReader(new InputStreamReader(System.in)));
            }
            for(Thread t : threads){
                t.join();
            }
            System.out.println("Work time : " + (System.currentTimeMillis() - time1));
            balancer.shutdown();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}