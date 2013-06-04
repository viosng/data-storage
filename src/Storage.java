/**
 * Created with IntelliJ IDEA.
 * User: vio
 * Date: 11.09.12
 */

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

public class Storage {
    private ConcurrentHashMap<Integer, Record> records;
    private String fileName;
    private TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    private TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
    public Storage(String fileName, ConcurrentHashMap<Integer, Record> rds){
        this.fileName = fileName;
        records = new ConcurrentHashMap<Integer, Record>();
        if(rds == null){
            Scanner scanner = null;
            try{
                File dbFile = new File(fileName + ".db");
                if(!dbFile.exists()){
                    dbFile.createNewFile();
                    return;
                }
                if(dbFile.length() == 0){
                    return;
                }
                scanner = new Scanner(dbFile);
                File file = new File(fileName + ".db");
                FileInputStream in = new FileInputStream(file);
                byte b[] = new byte[(int) file.length()];
                in.read(b);
                in.close();
                RecordList recordList = new RecordList();
                deserializer.deserialize(recordList, b);
                if(recordList.getRecords() != null){
                    for(Record rd : recordList.getRecords()){
                        records.put(rd.id, rd);
                    }
                }
            } catch(Exception e){
                e.printStackTrace();
            } finally {
                if(scanner != null){
                    scanner.close();
                }
            }
        } else {
            records.putAll(rds);
        }
    }

    public void flush(){
        try {
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(fileName + ".db"));
            RecordList list = new RecordList();
            for (Record record : records.values()) {
                list.addToRecords(record);
            }
            out.write(serializer.serialize(list));
            out.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void setFileName(String newFileName){
        fileName = newFileName;
    }

    public void add(Record record){
        records.put(record.id, record);
    }

    public void remove(int id){
        records.remove(id);
    }

    public Collection<Record> getValues(){
       return records.values();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Storage storage = (Storage) o;

        if (records != null ? !records.equals(storage.records) : storage.records != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return records != null ? records.hashCode() : 0;
    }

    public void update(int id, String updField, String updValue){
        Record record = records.get(id);
        if(record == null){
            return;
        }
        updField = updField.toLowerCase();
        Record._Fields field = Record._Fields.findByName(updField);
        if(field != null && record != null){
            record.setFieldValue(field, updValue);
        }
    }

    public List<Record> read(){
        ArrayList<Record> res = new ArrayList<Record>();
        for(Record rd : records.values()){
            res.add(rd);
        }
        return res;
    }

    public Record read(int id){
        return (records.containsKey(id)) ? records.get(id) :  new Record(-1, "", "");
    }

    public ConcurrentHashMap<Integer, Record> getRecords(){
        return records;
    }
}
