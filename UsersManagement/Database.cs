using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;

namespace UsersManagement
{
    class DBLinqModel
    {
        public List<object> fields = new List<object>();
        public List<bool> isInts = new List<bool>();

        public void addField(object field, bool isInt)
        {
            fields.Add(field);
            isInts.Add(isInt);
        }
    }


    class DBLinq
    {
        private string connectionStr;
        private DataSet dataSet = new DataSet();
        private DataTable dataTable = new DataTable();

        private string SEPRATOR = " , ";
        private string COTATION = "'";

        /**constants for queries**/
        private const string VARCHAR = " VARCHAR ";
        private const string INTEGER = " INT ";
        private const string FLOAT = " FLOAT ";
        private const string BOOLEAN = " BOOLEAN ";

        private NpgsqlConnection connection;

        public DBLinq()
        {
        }

        public void initializeConnection(string conn)
        {
            // Making connection with Npgsql provider
            connection = new NpgsqlConnection(conn);
        }

        /// <summary>
        /// connect to DB based on connection string that have given!
        /// </summary>
        public void connectToDB()
        {
            connection.Open();
        }

        /// <summary>
        /// closing the connection!
        /// </summary>
        public void closeConnection()
        {
            connection.Close();
        }


        /// <summary>
        /// return the whole of desired table! 
        /// type => DataSet
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DataSet getDataSet(string tableName)
        {
            string sql = "SELECT * FROM " + tableName;
            // data adapter making request from our connection
            NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(sql, connection);
            // i always reset DataSet before i do
            // something with it.... i don't know why :-)
            dataSet.Reset();
            // filling DataSet with result from NpgsqlDataAdapter
            adapter.Fill(dataSet);

            return dataSet;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataSet getDataSet(string tableName, string condition)
        {
            string sql = "SELECT * FROM " + tableName + " WHERE " + condition;
            // data adapter making request from our connection
            NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(sql, connection);
            // i always reset DataSet before i do
            // something with it.... i don't know why :-)
            dataSet.Reset();
            // filling DataSet with result from NpgsqlDataAdapter
            Console.WriteLine("SQLLL : " + sql);
            adapter.Fill(dataSet);


            return dataSet;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataSet delete(string tableName, string condition)
        {
            string sql = "DELETE FROM " + tableName + " WHERE " + condition;
            // data adapter making request from our connection
            NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(sql, connection);
            // i always reset DataSet before i do
            // something with it.... i don't know why :-)
            dataSet.Reset();
            // filling DataSet with result from NpgsqlDataAdapter
            adapter.Fill(dataSet);

            return dataSet;
        }


        /// <summary>
        /// inserting the new data!
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="values"></param>
        public void insert(string tableName, DBLinqModel model)
        {

            /* we make query string here */
            object[] fields = model.fields.ToArray();
            bool[] isInts = model.isInts.ToArray();

            string sql = "INSERT INTO " + tableName + " VALUES ( ";
            //getting type of field and adding to sql string query !
            for (int i = 0; i < fields.Length; i++)
            {
                if (!isInts[i])
                {
                    string sqlAppend = COTATION + fields[i] + COTATION + SEPRATOR;
                    sql = sql + sqlAppend;
                }
                else
                {
                    string sqlAppend = fields[i] + SEPRATOR;
                    sql = sql + sqlAppend;
                }
            }
            // removing the last charcter of sql that is ','
            sql = sql.Remove(sql.Length - 1);
            sql = sql.Remove(sql.Length - 1);
            sql = sql + ")";
            /* end of making sql query string! */

            Console.WriteLine("sql = \n" + sql);

            NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(sql, connection);
            // i always reset DataSet before i do
            // something with it.... i don't know why :-)
            dataSet.Reset();
            // filling DataSet with result from NpgsqlDataAdapter
            adapter.Fill(dataSet);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataSet update(string tableName, string[] attributes, string[] values, string condition)
        {
            string sql = "UPDATE " + tableName + " SET ";
            for (int i = 0; i < attributes.Length; i++)
            {
                sql = sql + attributes[i] + " = " + values[i] + " , ";
            }
            sql = sql.Remove(sql.Length - 1);
            sql = sql.Remove(sql.Length - 1);

            sql = sql + "where " + condition;

            // data adapter making request from our connection
            NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(sql, connection);
            // i always reset DataSet before i do
            // something with it.... i don't know why :-)
            dataSet.Reset();
            // filling DataSet with result from NpgsqlDataAdapter
            adapter.Fill(dataSet);

            return dataSet;
        }
    }
}
