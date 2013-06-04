import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created with IntelliJ IDEA.
 * User: vio
 * Date: 25.11.12
 */
public class Shard implements ShardService.Iface {
    private static class Server implements Runnable{

        TServer server;
        public final int port;

        public Server(ShardService.Iface handler, int port) throws TTransportException {
            this.port = port;
            final TServerTransport serverTransport = new TServerSocket(port);
            server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(new ShardService.Processor(handler)));
        }

        @Override
        public void run() {
            System.out.println("Shard Server on port " + port + " started");
            server.serve();
        }
    }

    private Server shardServer;
    private Storage master;
    private ArrayList<Storage> slaves;
    private FileWriter log;
    private FileWriter recoveryLog;
    private String dirName;
    private static int maxOperationCount = 200;
    private int operationsCount = maxOperationCount;
    private ConcurrentLinkedQueue<Storage> readQueue;
    private Thread serverThread;

    public Shard(int port, int slavesCount){
        this.dirName = "Shard " + Integer.toString(port);
        slaves = new ArrayList<Storage>();
        File myPath = new File(dirName + "/");
        myPath.mkdirs();
        master = new Storage(dirName + "/Master", null);
        // добавим несохраненные изменения
        addLastChanges(dirName + "/log.txt", master);
        Storage recovery = new Storage(dirName + "/Recovery", null);
        /// добавим изменения последнего сеанса
        addLastChanges(dirName + "/RecoveryLog.txt", recovery);
        if(!master.getRecords().entrySet().equals(recovery.getRecords().entrySet())){
            master = recovery;
            master.setFileName(dirName + "/Master");
        } else {
            recovery = null;
        }
        master.flush();
        // сохраним самую последнюю версию хранилища
        try {
            FileInputStream fis = new FileInputStream(dirName + "/Master.db");
            FileOutputStream fos = new FileOutputStream(dirName + "/Recovery.db");
            FileChannel fcin = fis.getChannel();
            FileChannel fcout = fos.getChannel();
            fcin.transferTo(0, fcin.size(), fcout);
            fcin.close();
            fcout.close();
            fis.close();
            fos.close();
            // создадим логи
            log = new FileWriter(dirName + "/log.txt");
            recoveryLog = new FileWriter(dirName + "/RecoveryLog.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // так как над всеми слейвами будем выполнять одинаковые операции, то добавим туда и мастер
        slaves.add(master);
        // создаем слейвы только для улучшения скорости чтения при многопоточной работе
        for(int i = 1; i <= slavesCount; i++){
            slaves.add(new Storage("", master.getRecords()));
        }
        try {
            shardServer = new Server(this, port);
            serverThread = new Thread(shardServer);
            serverThread.start();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        // записываем в очередь на чтение слейвы, мастер в конце, дабы сильно не нагружать
        readQueue = new ConcurrentLinkedQueue<Storage>();
        for(int i = 1; i <= slavesCount; i++){
            readQueue.add(slaves.get(i));
        }
        readQueue.add(master);
    }

    private void addLastChanges(String fileName, Storage storage){      //dirName + "/log.txt"
        try {
            File file = new File(fileName);
            if(!file.exists()){
                file.createNewFile();
            }
            Scanner logIn = new Scanner(new File(fileName));
            while (logIn.hasNextLine()) {
                argAnalyzer(logIn.nextLine().split(" "), storage);
            }
            logIn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void argAnalyzer(String args[], Storage storage){
        if(args == null || (args.length == 0)){
            return;
        }
        String command = args[0].toLowerCase();
        int id = Integer.parseInt(args[1]);
        if(command.equals( "add")){
            storage.add(new Record(id, args[2], args[3]));
        }
        else if(command.equals( "del")){
            storage.remove(id);
        }
        else {
            storage.update(id, args[2], args[3]);
        }
    }

    private void logIt(String s) {
        try {
            if (log == null) {
                log = new FileWriter(dirName + "/log.txt");
            }
            log.write(s);
            log.flush();
            recoveryLog.write(s);
            recoveryLog.flush();
        }
        catch (FileNotFoundException e) {
            System.out.println("Shard can't write in log!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flush(){
        master.flush();
        try {
            log.close();
            log = new FileWriter(dirName + "/log.txt");
            operationsCount = maxOperationCount;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleEnd(){
        operationsCount--;
        if(operationsCount == 0){
            flush();
            operationsCount = maxOperationCount;
        }
    }

    @Override
    public void add(Record record) throws TException {
        logIt(String.format("add %d %s %s\n", record.getId(), record.getName(), record.getPhone()));
        for(Storage slave :slaves){
            slave.add(record);
        }
        handleEnd();
    }

    @Override
    public Record read(int id) throws TException {
        Storage slave = readQueue.poll();
        readQueue.add(slave);
        return slave.read(id);
    }

    @Override
    public List<Record> readAll() throws TException {
        Storage slave = readQueue.poll();
        readQueue.add(slave);
        return slave.read();
    }

    @Override
    public void update(int id, String updfield, String updvalue) throws TException {
        logIt(String.format("update %d %s %s\n", id, updfield, updvalue));
        for(Storage slave :slaves){
            slave.update(id, updfield, updvalue);
        }
        handleEnd();
    }

    @Override
    public void remove(int id) throws TException {
        logIt(String.format("del %d\n", id));
        for(Storage slave :slaves){
            slave.remove(id);
        }
        handleEnd();
    }

    @Override
    public void shutdown() throws TException {
        flush();
        try {
            log.close();
            recoveryLog.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        shardServer.server.stop();
        serverThread.interrupt();
    }

    @Override
    public boolean isServe() throws TException {
        return shardServer.server.isServing();
    }

    private boolean equalDefiner(int a, int b, boolean equal){
        return equal ? a == b : a != b;
    }

    private List<Record> setDefiner(int r, int n, boolean equal) throws TException {
        ArrayList<Record> res = new ArrayList<Record>();
        for(Record rd : master.getValues()){
            if(equalDefiner(rd.getId() % n, r, equal)){
                res.add(rd);
            }
        }
        for(Record rd : res){
            remove(rd.getId());
        }
        return res;
    }

    @Override
    public List<Record> normalize(int r, int n) throws TException {
        return setDefiner(r, n, false);
    }

    @Override
    public List<Record> getWrong(int r, int n) throws TException {
        return setDefiner(r, n, true);
    }

    @Override
    public int getMaxId() throws TException {
        int max = -1;
        for(Record rd : master.getValues()){
            if(rd.getId() > max){
                max = rd.getId();
            }
        }
        return max;
    }
}
