using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mark2k.Entities
{
    public class DbQueryParams
    {
        public DbQueryParams()
        {
            this.Operator = QueryOperator.AND;
            this.SkipRecords = 0;
            this.Count = 50;
        }

        /// <summary>
        /// List of query parameters to search by in a form of key:value pairs, while key represents parameter's value type, e.g. "target" for target name or "artist" for artist name
        /// For example {{"target, "target name"}, {"artist", "artist name"}}
        /// </summary>
        public Dictionary<string, string> QueryParams { get; set; }
        
        /// <summary>
        /// AND or OR operator for the query. <i>Default is AND</i>
        /// </summary>
        public QueryOperator Operator { get; set; }

        /// <summary>
        /// Query params for sorting in a form of key:value where key is a field name to sort and value is a boolean value for sort direction. true:Ascending, false: Descending
        /// For example: <i>{{Info.Name, true}, {RecordInfo.UserCreated, false}}</i>
        /// </summary>
        public Dictionary<string, bool> SortParams { get; set; }

        /// <summary>
        /// Number of records to skip while querying the data. Use it for paging. <i>Default is 0</i>
        /// </summary>
        public int SkipRecords { get; set; }

        /// <summary>
        /// Number of records to return. <i>Default is 50</i>
        /// </summary>
        public int Count { get; set; }

        public DbQueryParams Clone()
        {
            return (DbQueryParams)this.MemberwiseClone();
        }
    }
}
