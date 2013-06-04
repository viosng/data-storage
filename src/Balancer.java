import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: vio
 * Date: 25.11.12
 */

public class Balancer implements BalancerService.Iface {

    private static class Server implements Runnable{

        TServer server;
        public final int port;

        public Server(BalancerService.Iface handler, int port) throws TTransportException {
            this.port = port;
            final TServerTransport serverTransport = new TServerSocket(port);
            server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(new BalancerService.Processor(handler)));
        }

        @Override
        public void run() {
            System.out.println("Balancer server started");
            server.serve();
        }
    }

    private Server balancerServer;
    private ArrayList<ShardService.Client> shards;
    private ConcurrentHashMap<Integer, ShardService.Client> shardFinder;
    private int nextId;
    private FileWriter logForCrashed;
    private ArrayList<Integer> ports;
    private int port;
    private String configName;
    private Thread serverThread;
    private HashSet<Integer> recordsHashSet;
    private boolean someAreCrashed = false;

    public Balancer(String configName){
        Scanner in = null;
        this.configName = configName;
        try {
            in = new Scanner(new File(configName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String config = "";
        while(in.hasNextLine()){
            config += in.nextLine() + " ";
        }
        in.close();
        String args[] = config.split("[a-zA-Z:, \n\t{}]+");
        int index = 1;
        port = Integer.parseInt(args[index++]);
        ports = new ArrayList<Integer>();
        shards = new ArrayList<ShardService.Client>();
        shardFinder = new ConcurrentHashMap<Integer, ShardService.Client>();
        try {
            balancerServer = new Server(this, port);
            serverThread = new Thread(balancerServer);
            serverThread.start();
            while(index < args.length){
                try{
                    int p = Integer.parseInt(args[index++]);
                    final TTransport transport = new TSocket("localhost", p);
                    transport.open();
                    ShardService.Client client = new ShardService.Client(new TBinaryProtocol(transport));
                    if(client.isServe()){
                        ports.add(p);
                        shards.add(client);
                    } else {
                        someAreCrashed = true;
                    }
                } catch(TException e){
                    someAreCrashed = true;
                }

            }
            // if balancer was crashed it will help to define new nextId, because of we save configs in the end
            for(ShardService.Client shard : shards){
                int max = shard.getMaxId() + 1;
                if(max > nextId){
                    nextId = max;
                }
            }
            recordsHashSet = new HashSet<Integer>();
            modifyMap();
            normalize();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        try {
            File file = new File("logForCrashed.log");
            if(!file.exists()){
                file.createNewFile();
            }
            addLastChanges(file);
            logForCrashed = new FileWriter(file);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void clearDuplicates(){
        recordsHashSet.clear();
        try {
            for(ShardService.Client shard : shards){
                List<Record> shardRecords = shard.readAll();
                for(Record rd : shardRecords){
                    if(recordsHashSet.contains(rd.hashCode())){
                        shard.remove(rd.id);
                    } else {
                        recordsHashSet.add(rd.hashCode());
                    }
                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void modifyMap(){
        shardFinder.clear();
        int n = shards.size();
        if(n == 0){
            return;
        }
        for(int i = 0; i < n; i++){
            shardFinder.put(i, shards.get(i));
        }
    }

    private void logIt(String query){
        try {
            logForCrashed.write(query + "\n");
            logForCrashed.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addLastChanges(File file){
        try {
            // копируем логи, по которым будем восстанавливать, так как при восстановлении действия также логируются
            Scanner logIn = new Scanner(file);
            StringBuilder lines = new StringBuilder();
            while(logIn.hasNextLine()){
                lines.append(logIn.nextLine() + "\n");
            }
            logIn.close();
            logIn = new Scanner(lines.toString());
            while (logIn.hasNextLine()) {
                argAnalyzer(logIn.nextLine().split(" "));
            }
            logIn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void argAnalyzer(String args[]) throws TException {
        String command = args[0].toLowerCase();
        if(command.equals( "add")){
            add(new Record(0, args[1], args[2]));
        }
        else if(command.equals( "del")){
            remove(Integer.parseInt(args[1]));
        }
        else if(command.equals( "update")){
            update(Integer.parseInt(args[1]), args[2], args[3]);
        }
    }

    @Override
    public synchronized void add(Record record) throws TException {
        if(recordsHashSet.contains(record.hashCode()) || shards.size() == 0){
            return;
        }
        int k = shards.size();
        int n = k;
        boolean wroten = false;
        if(someAreCrashed){
            logIt(String.format("add %s %s", record.getName(), record.getPhone()));
            wroten = true;
        }
        while(k-- > 0){
            ShardService.Client shard = shardFinder.get(nextId % n);
            if(shard.isServe()){
                recordsHashSet.add(nextId);
                record.setId(nextId++);
                shard.add(record);
                return;
            }
            nextId++;
        }
        nextId -= k;
        if(!wroten){
            logIt(String.format("add %s %s", record.getName(), record.getPhone()));
        }
    }

    @Override
    public synchronized Record read(int id) throws TException {
        if(shards.size() == 0){
            return new Record(-1, "", "");
        }
        ShardService.Client shard = shardFinder.get(id % shards.size());
        return shard.isServe() ? shard.read(id) : null;
    }

    @Override
    public synchronized List<Record> readAll() throws TException {
        if(shards.size() == 0){
            return new ArrayList<Record>();
        }
        ArrayList<Record> res = new ArrayList<Record>();
        for(ShardService.Client shard : shards){
            List<Record> shardRecords = shard.readAll();
            if(shardRecords != null){
                res.addAll(shardRecords);
            }
        }
        return res;
    }

    @Override
    public synchronized void update(int id, String updfield, String updvalue) throws TException {
        if(shards.size() == 0){
            return;
        }
        if(someAreCrashed){
            logIt(String.format("update %d %s %s", id, updfield, updvalue));
        }
        ShardService.Client shard = shardFinder.get(id % shards.size());
        if(shard.isServe()){
            Record record = shard.read(id);
            if(record.id == -1){
                return;
            }
            recordsHashSet.remove(record.hashCode());
            shard.update(id, updfield, updvalue);
            recordsHashSet.add(shard.read(id).hashCode());
        } else {
            logIt(String.format("update %d %s %s", id, updfield, updvalue));
        }
    }

    @Override
    public synchronized void remove(int id) throws TException {
        if(shards.size() == 0){
            return;
        }
        if(someAreCrashed){
            logIt(String.format("del %d", id));
        }
        ShardService.Client shard = shardFinder.get(id % shards.size());
        if(shard.isServe()){
            recordsHashSet.remove(shard.read(id));
            shard.remove(id);
        } else {
            logIt(String.format("del %d", id));
        }
    }

    private void beforeRestruct() throws TException {
        ArrayList<ShardService.Client> newShards = new ArrayList<ShardService.Client>();
        ArrayList<Integer> newPorts = new ArrayList<Integer>();
        for(int i = 0; i < shards.size(); i++){
            ShardService.Client shard = shards.get(i);
            if(shard.isServe()){
                newShards.add(shard);
                newPorts.add(ports.get(i));
            }
        }
        if(shards.size() > newShards.size()){
            shards = newShards;
            ports = newPorts;
        }
    }

    public void normalize() throws TException {
        beforeRestruct();
        int n = shards.size();
        if(n == 0){
            return;
        }
        for(int i = 0; i < n - 1; i++){
            ShardService.Client shard = shards.get(i);
            ArrayList<Record> wrongRecords = (ArrayList<Record>) shard.normalize(i, n);
            for(Record record : wrongRecords){
                shardFinder.get(record.getId() % n).add(record);
            }
            for(int j = i + 1; j < n; j++){
                wrongRecords = (ArrayList<Record>) shards.get(j).getWrong(i, n);
                for(Record record: wrongRecords){
                    shard.add(record);
                }
            }
        }
        modifyMap();
        clearDuplicates();
    }

    public void addShard(int port) throws TException {
        for(Integer p : ports){
            if(p.intValue() == port){
                return;
            }
        }
        final TTransport transport = new TSocket("localhost", port);
        transport.open();
        ShardService.Client client = new ShardService.Client(new TBinaryProtocol(transport));
        if(!client.isServe()){
            return;
        }
        shards.add(client);
        ports.add(port);
        modifyMap();
        normalize();
        createConfig();
    }

    public void removeShard(int port) throws TException {
        beforeRestruct();
        int index = -1;
        int n = shards.size();
        for(int i = 0; i < n; i++){
            if(ports.get(i).intValue() == port){
                index = i;
                ShardService.Client shard = shards.get(index);
                if(!shard.isServe()){
                    return;
                }
                ArrayList<Record> records = (ArrayList<Record>) shard.readAll();
                n--;
                shards.remove(index);
                ports.remove(index);
                modifyMap();
                for(Record record : records){
                    recordsHashSet.remove(record.hashCode());
                }
                for(Record record : records){
                    add(record);
                }
                normalize();
                createConfig();
                return;
            }
        }
    }

    public void createConfig(){
        PrintWriter out = null;
        try {
            out = new PrintWriter(configName);
            out.write("{\n");
            out.write(String.format("\tPort : %d,\n", port));
            out.write("\tShardPorts : {\n");
            for(int i = 0; i < shards.size(); i++){
                out.write(String.format("\t\t%d,\n",ports.get(i)));
            }
            out.write("\t}\n");
            out.write("}");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if(out != null){
                out.close();
            }
        }
    }

    public void shutdown() throws IOException, TException {
        logForCrashed.close();
        for(ShardService.Client shard : shards){
            shard.shutdown();
        }
        balancerServer.server.stop();
        serverThread.interrupt();
    }

    public int getPort() {
        return port;
    }

    public ArrayList<Integer> getPorts() {
        return ports;
    }
}
