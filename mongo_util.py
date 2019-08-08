from pymongo import MongoClient


class BFMongo(object):
    def __init__(self, ip, port, user, password):
        try:
            from urllib.parse import quote_plus
        except:
            from urllib import quote_plus
        self._uri = f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{ip}:{port}"
        print(f"URL:{self._uri}")
        self._client = MongoClient(self._uri)
        self._database = None

    def document_utils(self):
        pass

    def collection_utils(self):
        pass

    def database_utils(self):
        database_names = self._client.database_names()
        print(f"database names:{database_names}")
        database_name = "example"
        if database_name in database_names:
            self._database = self._client.get_database(database_name)
        collections_names = self._database.collection_names()
        print(f"collection names:{collections_names}")

        new_collection_name = "new_collection"
        # result = self._database.create_collection(new_collection_name)
        # print(f"create collection name:{new_collection_name} result:{result}")
        new_collection = self._database.get_collection(new_collection_name)
        document = {"name": "her", "born": 2034}
        documents = [{"name": "lee", "password":"iot"}, {"name": "test", "password": "tex", "age": 23}]
        result = new_collection.insert_one(document)
        print(f"insert one collection result:{result}")
        result = new_collection.insert_many(documents)
        print(f"insert many collection result:{result}")


if __name__ == "__main__":
    ip = "10.160.34.113"
    port = 27018
    user = "lee"
    password = "Befast@1989"
    bf_mongo = BFMongo(ip, port, user, password)
    bf_mongo.database_utils()
