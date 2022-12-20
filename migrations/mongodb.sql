-- Данные в clients
db.getSiblingDB("creator").getCollection("clients").insertOne(
   { _id: 79000000001, gender: "M", age: 20, income: 10000.00, nextuse: "2022-12-30"}
);

-- Данные в segments
db.getSiblingDB("creator").getCollection("segments").insertMany([
    { id: "12345678-1234-5678-1234-567812345678", msisdn: 79000000001},
    { id: "12345678-1234-5678-1234-567812345678", msisdn: 79000000002}
]);


-- Индексы
db.getSiblingDB("creator").getCollection("clients").createIndex({counter: 1});
db.getSiblingDB("creator").getCollection("segments").createIndex({id: 1});

-- Выборка для сегмента
db.getSiblingDB("creator").getCollection("clients").find({}).sort({"counter": 1}).limit(10);

-- Выборка из сегмента
db.getSiblingDB("creator").getCollection("segments").find({id: "12345678-1234-5678-1234-567812345678"});
db.getSiblingDB("creator").getCollection("segments").find({id: "12345678-1234-5678-1234-567812345678"},{msisdn:1, _id:0});

-- Обновление каунтеров
db.getSiblingDB("creator").getCollection("clients").find({}).sort({"counter": 1}).limit(10).map(
    function(doc) {
        return doc._id;
    }
);

db.getSiblingDB("creator").getCollection("clients").updateMany({_id: {$in: [79000000001,79000000002]}}, {$inc: {counter:1}});



