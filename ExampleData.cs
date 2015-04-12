using mark2k.Entities;
using mark2k.models;
using mark2k.Mongo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mark2k
{
    public class ExampleData
    {
        private MongoSession MongoSession
        {
            get { return new MongoSession("testDb"); }
        }

        /// <summary>
        /// Explicit Add
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        public User Add(User user)
        {
            MongoSession.Add<User>(user);
            return user;
        }

        /// <summary>
        /// Save or Add if not exists
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        public User Edit(User user)
        {
            MongoSession.Save<User>(user);
            return user;
        }

        public User FindById(string id)
        {
            return MongoSession.Single<User>(t => t.id == id);
        }

        public List<User> GetAll()
        {
            return MongoSession.All<User>().ToList();
        }

        public void Delete(string id)
        {
            MongoSession.Delete<User>(id);
        }

        public List<User> LambdaSearch(string str)
        {
            return MongoSession.Select<User>(t => t.Name.ToLower().Contains(str))
                .OrderBy(t => t.Name)
                .ToList();
        }

        public List<User> Search(string str)
        {
            Dictionary<string, string> searchParams = new Dictionary<string, string>();
            searchParams.Add("Name", str);

            return MongoSession.Search<User>(searchParams);
        }

        public List<User> GetPage(DbQueryParams qParams)
        {
            return MongoSession.Page<User>(qParams);
        }

    }
}
