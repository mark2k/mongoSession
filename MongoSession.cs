using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GeoJsonObjectModel;
using System.Text.RegularExpressions;
using MongoDB.Bson.Serialization;
using mark2k.Entities;

namespace mark2k.Mongo
{
    public class MongoSession
    {
        private MongoDatabase mongoDb;  // = new MongoClient(ConfigurationProvider.DefaultConfiguration.MongoDb.ConnectionString).GetServer().GetDatabase(ConfigurationProvider.DefaultConfiguration.MongoDb.DbName);

        public MongoSession(string connectionString, string dbName)
        {
            // *******
            // ConnectionString and dbName can (and should) be read from the configuration file
            // *******
            mongoDb = new MongoClient(connectionString).GetServer().GetDatabase(dbName);
        }

        public MongoSession(string dbName)
        {
            mongoDb = new MongoClient(String.Format("mongodb://127.0.0.1:27017/{0}?safe=true", dbName)).GetServer().GetDatabase(dbName);
        }

        public void Delete<T>(System.Linq.Expressions.Expression<Func<T, bool>> expression) where T : class, new()
        {
            var items = mongoDb.GetCollection(GetTypeName(typeof(T))).AsQueryable<T>().Where(expression);
        }

        public void Delete<T>(string id) where T : class, new()
        {
            mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Remove(MongoDB.Driver.Builders.Query.EQ("_id", id));
        }

        public void DropCollection<T>() where T : class, new()
        {
            mongoDb.DropCollection(GetTypeName(typeof(T)));
        }

        public T Single<T>(System.Linq.Expressions.Expression<Func<T, bool>> expression) where T : class, new()
        {
            T retval = default(T);
            retval = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).AsQueryable().SingleOrDefault(expression);
            return retval;
        }

        public List<T> Select<T>(System.Linq.Expressions.Expression<Func<T, bool>> expression) where T : class, new()
        {
            List<T> retval = default(List<T>);            
            retval = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).AsQueryable().Where(expression).ToList();
            return retval;
        }

        public List<T> Page<T>(DbQueryParams qParams) where T : class, new()
        {
            // First query part consists of search params with specified operator between
            List<IMongoQuery> queries = new List<IMongoQuery>();
            foreach (var item in qParams.QueryParams)
                queries.Add(Query.Matches(item.Key, new BsonRegularExpression(String.Format("(?:{0})", item.Value), "is")));

            var query = qParams.Operator == QueryOperator.AND ? Query.And(queries) : Query.Or(queries);

            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(query);
            cursor.SetSkip(qParams.SkipRecords).SetLimit(qParams.Count);

            // Setting the Sort params
            if (qParams.SortParams != null && qParams.SortParams.Count > 0)
            {
                SortByBuilder sbb = new SortByBuilder();
                foreach (KeyValuePair<string, bool> sortItem in qParams.SortParams)
                {
                    if (sortItem.Value)
                        sbb.Ascending(sortItem.Key);
                    else
                        sbb.Descending(sortItem.Key);
                }
                cursor.SetSortOrder(sbb);
            }

            return cursor.ToList();
        }

        public List<T> Page<T>(int skip, int limit, string orderBy) where T : class, new()
        {
            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).FindAll();
            cursor.SetSkip(skip).SetLimit(limit);  
            
            if(!String.IsNullOrEmpty(orderBy))
                cursor.SetSortOrder(SortBy.Descending(orderBy));
            return cursor.ToList();
        }

        public List<T> Page<T>(System.Linq.Expressions.Expression<Func<T, bool>> expression, int skip, int limit, string orderBy) where T : class, new()
        {
            IMongoQuery query = Query<T>.Where(expression);
            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(query);
            cursor.SetSkip(skip).SetLimit(limit);

            if (!String.IsNullOrEmpty(orderBy))
                cursor.SetSortOrder(SortBy.Descending(orderBy));

            return cursor.ToList();
        }

        public List<T> Search<T>(Dictionary<string, string> searchparams) where T : class, new()
        {
            return Search<T>(searchparams, QueryOperator.OR);
        }
        /// <summary>
        /// Perform Regex match search on given fields with OR between them and return first 50 results
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="searchparams"></param>
        /// <returns></returns>
        public List<T> Search<T>(Dictionary<string, string> searchparams, QueryOperator qOperator) where T : class, new()
        {
            List<IMongoQuery> queries = new List<IMongoQuery>();
            foreach (var item in searchparams)
                queries.Add(Query.Matches(item.Key, new BsonRegularExpression(String.Format("(?:{0})", item.Value), "is")));

            var query = qOperator == QueryOperator.AND ? Query.And(queries) : Query.Or(queries);
            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(query);
            
            // ***** optionally set limit. use parameter instead of 50 *****
            //cursor.SetLimit(50);
            
            return cursor.ToList();
        }

        /// <summary>
        /// Perform a full text query on all fields indexed as text. 
        /// Returns first 50 results
        /// </summary>
        /// <returns>First 50 results</returns>
        public List<T> TextSearch<T>(string searchStr) where T : class, new()
        {
            return TextSearch<T>(searchStr, 0, 50, null);
        }

        /// <summary>
        /// Perform a full text query on all fields indexed as text. 
        /// </summary>
        /// <returns>First 50 results</returns>
        public List<T> TextSearch<T>(string searchStr, int skip, int limit, string orderBy) where T : class, new()
        {
            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(Query.Text(searchStr));

            if (skip > 0)
                cursor.SetSkip(skip);

            cursor.SetLimit(limit);
            
            if(orderBy != null)
                cursor.SetSortOrder(SortBy.Descending(orderBy));

            return cursor.ToList();
        }


        /// <summary>
        /// Perform exact RegEx search
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="searchparams"></param>
        /// <returns>First 50 results</returns>
        public List<T> SearchExact<T>(Dictionary<string, object> searchparams) where T : class, new()
        {
            return SearchExact<T>(searchparams, QueryOperator.AND);
        }
        /// <summary>
        /// Perform exact RegEx search
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="searchparams"></param>
        public List<T> SearchExact<T>(Dictionary<string, object> searchparams, QueryOperator qOperator) where T : class, new()
        {
            List<IMongoQuery> queries = new List<IMongoQuery>();
            foreach (var item in searchparams)
            {
                if(item.Value == null)
                    queries.Add(Query.EQ(item.Key, BsonNull.Value));
                else
                    queries.Add(Query.Matches(item.Key, new BsonRegularExpression(String.Format("(^{0}$)", item.Value), "i")));
            }

            var query = qOperator == QueryOperator.AND ? Query.And(queries) : Query.Or(queries);
            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(query);
            return cursor.ToList();
        }

        /// <summary>
        /// Search for documents containing the searched value in a simple named array e.g. { names: ["john", "sarah", "matt"] }
        /// Same as MongoSession.Select<item>(p => p.Relations.Contains(id));
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public List<T> SearchArray<T>(string arrayName, string value) where T : class, new()
        {
            // might need to change "$eq" for "$in"
            string command = "{ \"" + arrayName + "\" : { $elemMatch : { $eq: \"" + value + "\" } } }";
            var bsonDoc = BsonSerializer.Deserialize<BsonDocument>(command);
            var query = new QueryDocument(bsonDoc);

            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(query);
            return cursor.ToList();
        }

        /// <summary>
        /// Search for documents containing the searched value, that is a field of an object in array of objects array e.g. { contacts: [{name: "john", id: 1}, {name: "sarah", id: 2}, {name: "matt", id=3}] }
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public List<T> SearchObjectArray<T>(string arrayName, string key, string value) where T : class, new()
        {
            string command = "{ \"" + arrayName + "\" : { $elemMatch : { \"" + key + "\": \"" + value + "\" } } }";
            var bsonDoc = BsonSerializer.Deserialize<BsonDocument>(command);
            var query = new QueryDocument(bsonDoc);

            MongoCursor<T> cursor = mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Find(query);
            return cursor.ToList();
        }

        public IQueryable<T> All<T>() where T : class, new()
        {
            return mongoDb.GetCollection<T>(GetTypeName(typeof(T))).AsQueryable();
        }

        public void Add<T>(T item) where T : class, new()
        {
            mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Insert(item);
        }

        public void Add<T>(IEnumerable<T> items) where T : class, new()
        {
            mongoDb.GetCollection<T>(GetTypeName(typeof(T))).InsertBatch(items);
        }

        public void Save<T>(T item) where T : class, new()
        {
            mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Save(item);
        }

        /// <summary>
        /// perform $nearSphere query
        // http://docs.mongodb.org/manual/reference/operator/query/nearSphere/
        /// </summary>
        /// <typeparam name="T">Collection to perform the query on</typeparam>
        /// <param name="geoField">path to the Geo document within main document. e.g. For Places document: "Geo", For Collection document: "Place.Geo"</param>
        /// <param name="longitude">Longitude of the point, near to which to perform the search</param>
        /// <param name="latitude">Latitude of the point, near to which to perform the search</param>
        /// <param name="maxDistance">Max distance from the point to search. In Meters</param>
        /// <returns>List of T</returns>
        public List<T> GeoNear<T>(string geoField, double longitude, double latitude, double maxDistance) where T : class, new() 
        {
            GeoJson2DGeographicCoordinates coordinates = new GeoJson2DGeographicCoordinates(longitude, latitude);
            MongoDB.Driver.GeoJsonObjectModel.GeoJsonPoint<GeoJson2DGeographicCoordinates> point = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(coordinates);
            IMongoQuery nearQuery = Query.Near(geoField, point, maxDistance, true);

            var collection = mongoDb.GetCollection<T>(GetTypeName(typeof(T)));
            var query = collection.Find(nearQuery);
            List<T> listings = query.ToList();
            return listings;
        }

        private string GetTypeName(Type type)
        {
            if (type.BaseType != typeof(Object))
                return type.BaseType.Name;

            return type.Name;
        }

        /// <summary>
        /// EnsureIndex for TTL(Time To Live) - Auto expiration feature 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public void EnsureTTLIndex<T>() where T : class, new()
        {
            IndexKeysDocument ikd = new IndexKeysDocument(new BsonElement("Date", 1));
            IndexOptionsDocument iod = new IndexOptionsDocument(new BsonElement("expireAfterSeconds", 21600)); // 6 hours
            mongoDb.GetCollection<T>(GetTypeName(typeof(T))).CreateIndex(ikd, iod);
        }

        public long Count<T>() where T : class, new()
        {
            return mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Count();
        }

        public long Count<T>(DbQueryParams qParams) where T : class, new()
        {
            // First query part consists of search params with specified operator between
            List<IMongoQuery> queries = new List<IMongoQuery>();
            foreach (var item in qParams.QueryParams)
                queries.Add(Query.Matches(item.Key, new BsonRegularExpression(String.Format("(?:{0})", item.Value), "is")));

            var query = qParams.Operator == QueryOperator.AND ? Query.And(queries) : Query.Or(queries);
            return mongoDb.GetCollection<T>(GetTypeName(typeof(T))).Count(query);
        }
    }
}
