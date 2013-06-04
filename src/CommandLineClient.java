/**
 * Created with IntelliJ IDEA.
 * User: vio
 * Date: 29.11.12
 */
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


import java.io.*;
import java.util.HashMap;
import java.util.List;

// Command line API
public class CommandLineClient implements Runnable{
    static TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    public interface Command {
        void handle(CommandLineClient commandLineClient, String args[]) throws TException;
    }

    final static HashMap < String, Command > commands;

    static {
        commands = new HashMap<String, Command>();
        commands.put("add", new Command() {
            @Override
            public void handle(CommandLineClient commandLineClient, String[] args) throws TException {
                if(args.length == 3){
                    String phoneNumber = args[2];
                    for(int i = (phoneNumber.charAt(0) == '+' ? 1 : 0) ; i < phoneNumber.length(); i++){
                        if(!Character.isDigit(phoneNumber.charAt(i))){
                            print("Wrong phone number syntax");
                            return;
                        }
                    }
                    commandLineClient.client.add(new Record(0, args[1], args[2]));
                }
                else print("Syntax error");
            }
        });
        commands.put("read", new Command() {
            @Override
            public void handle(CommandLineClient commandLineClient, String[] args) throws TException {
                if(args.length != 2){
                    print("Syntax error");
                } else {
                    if(!args[1].equals("*")){
                        int index;
                        try{
                            index = Integer.parseInt(args[1]);
                        } catch (NumberFormatException e){
                            print("Syntax error");
                            return;
                        }
                        if(index < 0){
                            return;
                        }
                        Record record = commandLineClient.client.read(index);
                        if(record.id != -1){
                            //print(serializer.toString(record));
                        }

                    }
                    else{
                        List<Record> records = commandLineClient.client.readAll();
                        print(Integer.toString(records.size()));
                        //for(Record record : records){
                        //    print(serializer.toString(record));
                        //}
                    }

                }
            }
        });
        commands.put("del", new Command() {
            @Override
            public void handle(CommandLineClient commandLineClient, String[] args) throws TException {
                if(args.length != 2){
                    print("Syntax error");
                } else {
                    int index;
                    try{
                        index = Integer.parseInt(args[1]);

                    } catch (NumberFormatException e){
                        print("Syntax error");
                        return;
                    }
                    commandLineClient.client.remove(index);
                }
            }
        });
        commands.put("update", new Command() {
            @Override
            public void handle(CommandLineClient commandLineClient, String[] args) throws TException {
                if(args.length != 4){
                    print("Syntax error");
                } else {
                    int index;
                    try{
                        index = Integer.parseInt(args[1]);

                    } catch (NumberFormatException e){
                        print("Syntax error");
                        return;
                    }
                    commandLineClient.client.update(index, args[2], args[3]);
                }
            }
        });
        commands.put("help", new Command() {
            @Override
            public void handle(CommandLineClient commandLineClient, String[] args) {
                if (args.length == 1) {
                    commandLineClient.help();
                } else {
                    print("Syntax error");
                }
            }
        });
    }

    BalancerService.Client client;
    int number;
    final TTransport transport;
    public CommandLineClient(int port, int number) throws TTransportException {
        this.number = number;
        transport = new TSocket("localhost", port);
        transport.open();
        client = new BalancerService.Client(new TBinaryProtocol(transport));
    }

    @Override
    public void run() {
        try{
            start(new BufferedReader(new FileReader("Files/File" + number + ".txt")));
            transport.close();
        } catch (FileNotFoundException e){
            e.printStackTrace();
        }

    }
    public void start(BufferedReader in){
        try {
            String s = in.readLine();
            while(s != null && !s.equals("q")){
                argAnalyzer(s.split(" "));
                s = in.readLine();
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Client %d is over", number));
    }

    private void argAnalyzer(String args[]) throws TException {
        if(args == null || (args.length == 0)){
            return;
        }
        String command = args[0].toLowerCase();
        if(CommandLineClient.commands.containsKey(command)){
            CommandLineClient.commands.get(command).handle(this, args);
        } else {
            print("Syntax error");
        }
    }

    static void print(String str){
        System.out.println(str);
    }

    private void help(){
        print("-----------My Distributed Database v 0.1--------");
        print("add <name> <phone>        ---     to add record to opened database");
        print("read <Id>       ---     to print record with such id");
        print("read *       ---     to print all records of database");
        print("update <Id> <Field> <Value>        ---     change value in field in record with such id");
        print("!!!You can't change field \"id\"");
        print("del <Id>      ---     delete record with such id");
        print("help     ---    print this information again");
        print("q  ---   quit");
        print("-----------------------------");
    }
}

