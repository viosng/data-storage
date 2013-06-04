struct Record {
    1: i32 id,
    2: string name,
    3: string phone
}
struct RecordList {
    1: list<Record> records
}
service BalancerService {
    void add(1: Record record),
    Record read(1: i32 id)
    list<Record> readAll(),
    void update(1: i32 id, 2: string updfield, 3: string updvalue),
    void remove(1: i32 id)
}
service ShardService {
    void add(1: Record record),
    Record read(1: i32 id)
    list<Record> readAll(),
    void update(1: i32 id, 2: string updfield, 3: string updvalue),
    void remove(1: i32 id),
    void shutdown(),
    bool isServe(),
    list<Record> normalize(1: i32 r, 2: i32 n),
    list<Record> getWrong(1: i32 r, 2: i32 n),
    i32 getMaxId()
}
