package edu.viosng.distribdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: vio
 * Date: 30.11.12
 */
public class AdminCommandLine {
    static TSerializer serializer = new TSerializer(new TSimpleJSONProtocol.Factory());
    public interface Command {
        void handle(AdminCommandLine adminCommandLine, String args[]) throws TException;
    }

    final static HashMap< String, Command > commands;

    static {
        commands = new HashMap<String, Command>();
        commands.put("add", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) throws TException {
                if(args.length == 3){
                    String phoneNumber = args[2];
                    for(int i = (phoneNumber.charAt(0) == '+' ? 1 : 0) ; i < phoneNumber.length(); i++){
                        if(!Character.isDigit(phoneNumber.charAt(i))){
                            print("Wrong phone number syntax");
                            return;
                        }
                    }
                    adminCommandLine.client.add(new Record(0, args[1], args[2]));
                }
                else print("Syntax error");
            }
        });
        commands.put("read", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) throws TException {
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
                        Record record = adminCommandLine.client.read(index);
                        if(record.id != -1){
                            print(serializer.toString(record));
                        }

                    }
                    else{
                        List<Record> records = adminCommandLine.client.readAll();
                        print(Integer.toString(records.size()));
                        for(Record record : records){
                            print(serializer.toString(record));
                        }
                    }

                }
            }
        });
        commands.put("del", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) throws TException {
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
                    adminCommandLine.client.remove(index);
                }
            }
        });
        commands.put("update", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) throws TException {
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
                    adminCommandLine.client.update(index, args[2], args[3]);
                }
            }
        });
        commands.put("q", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) {
                try {
                    adminCommandLine.balancer.shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TException e) {
                    e.printStackTrace();
                }
            }
        });

        commands.put("help", (adminCommandLine, args) -> {
            if (args.length == 1) {
                adminCommandLine.help();
            } else {
                print("Syntax error");
            }
        });
        commands.put("shards", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) {
                for(Integer port : adminCommandLine.balancer.getPorts()){
                    print(port.toString());
                }
            }
        });
        commands.put("append", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) {
                int port;
                try{
                    port = Integer.parseInt(args[1]);

                } catch (NumberFormatException e){
                    print("Syntax error");
                    return;
                }
                if(args.length == 2){
                    try {
                        adminCommandLine.balancer.addShard(port);
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }  else {
                    print("Syntax error");
                }
            }
        });
        commands.put("kick", new Command() {
            @Override
            public void handle(AdminCommandLine adminCommandLine, String[] args) {
                int port;
                try{
                    port = Integer.parseInt(args[1]);

                } catch (NumberFormatException e){
                    print("Syntax error");
                    return;
                }
                if(args.length == 2){
                    try {
                        adminCommandLine.balancer.removeShard(port);
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }  else {
                    print("Syntax error");
                }
            }
        });
    }

    BalancerService.Client client;
    Balancer balancer;
    int number;
    final TTransport transport;
    public AdminCommandLine(Balancer balancer) throws TTransportException {
        this.balancer = balancer;
        transport = new TSocket("localhost", balancer.getPort());
        transport.open();
        client = new BalancerService.Client(new TBinaryProtocol(transport));
    }

    public void start(BufferedReader in){
        try {
            String s = in.readLine();
            while(!s.equals("q")){

                if(s == null){
                    break;
                }
                argAnalyzer(s.split(" "));
                s = in.readLine();
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Admin session is over", number));
    }

    private void argAnalyzer(String args[]) throws TException {
        if(args == null || (args.length == 0)){
            return;
        }
        String command = args[0].toLowerCase();
        if(AdminCommandLine.commands.containsKey(command)){
            AdminCommandLine.commands.get(command).handle(this, args);
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
        print("shards --- print shard's ports");
        print("kick <#> --- delete shard from balancer on port #");
        print("append <#> --- add shard to balancer on port #");
        print("q  ---   quit");
        print("-----------------------------");
    }
}
