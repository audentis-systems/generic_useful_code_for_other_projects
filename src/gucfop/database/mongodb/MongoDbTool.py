from pymongo import MongoClient

class MongoDbTool():

    #
    # constructor
    #
    def __init__(
        self,
        connection_string : str,
        database_name : str,
        collection_name : str,
        serverSelectionTimeoutMS = 5000,
    ):
        self.connect(connection_string, serverSelectionTimeoutMS)
        self.database_name = database_name
        self.collection_name = collection_name
        self.database = self.client[self.database_name]
        self.collection = self.database[collection_name]

    #
    # deconstructor
    #
    def __del__(self):
        self.client.close()
    
    #
    # connect to MongoDB
    #
    def connect(
        self,
        connection_string,
        serverSelectionTimeoutMS,
        verbose = False,
    ):
        self.client = MongoClient(connection_string, serverSelectionTimeoutMS)
        if verbose:
            self.client.admin.command("ping")
            print("Successfully connected to MongoDB.")

    #
    # delete the class's database
    #
    def delete_database(self):
        self.client.drop_database(self.database_name)

    #
    # delete the class's collection
    #
    def delete_collection(self):
        self.collection.drop()

    #
    # query the class's collection
    #
    def query_collection(self, query : dict):
        return self.collection.find(query)

    #
    # insert one dictionary into the class's collection
    #
    def insert_one(self, document : dict):
        return self.collection.insert_one(document)

    #
    # insert many dictionaries into the class's collection
    #
    def insert_many(self, list_to_insert : list):
        if len(list_to_insert) > 0:
            insert_result = self.collection.insert_many(list_to_insert)
        return insert_result

#
# test
#
def test():
    connection_string = 'mongodb://localhost:27017'
    mdb = MongoDbTool(connection_string, 'test_database', 'test_collection')
    print()
    mdb.delete_collection()
    mdb.delete_database()

    a = mdb.insert_one({"name": "Jane Doe", "address": "123 Main St"})
    b = mdb.insert_one({"name": "Sally Jane", "address": "123 Main St"})
    c = mdb.insert_one({"name": "John Doe", "address": "124 Main St"})
    print(a, b, c)
    print()
    
    a = mdb.insert_many(
        [
            {"name": "Jane Doe 2", "address": "125 Main St"},
            {"name": "Sally Jane 2", "address": "125 Main St"},
            {"name": "John Doe 2", "address": "126 Main St"},
        ]
    )
    print(a)
    print()

    query_dict = {}
    the_documents = mdb.query_collection(query_dict)
    print(type(the_documents))
    for doc in the_documents:
        print(doc)

    mdb.delete_database()
    del(mdb)
