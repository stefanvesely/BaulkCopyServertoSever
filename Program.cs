using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace BaulkCopyServertoSever
{
    /*This Application will download everything from one DataBase to Another one with the same schema. What I recommend is backing up 
     * the schema first then uploading the schema and then running this application */
    internal class Program
    {
        public string sGetTables = @"SELECT t.name as TableName, SCHEMA_NAME(t.schema_id) As SchemaName,I.rows as RecordCount from sysindexes i INNER JOIN sys.tables t on i.id=t.object_id";
        public string sConStringDownLoad = @"";//put your source connections string in here 
        public string sConStringUpload = @"";//put your destination summary in here
        public List<string> lstTableNames = new List<string>();

        public static void  Main()
        {
            var MainProgram = new Program();
            MainProgram.MainFunction();
        }
        public void MainFunction()
        {
            Console.WriteLine("Working");
            GetTableNames();
            foreach (string sTableName in lstTableNames)
            {
                DataTable dtWorkingTable = GetSingleTable(sTableName);
                SuperBulkUploadTable(sTableName, dtWorkingTable, sConStringUpload);
            }
            Console.WriteLine("Done.");
        }
        /// <summary>
        /// Get Names
        /// Gets all the names of tables in your source DB
        /// </summary>
        private void GetTableNames()
        {
            DataTable dtTableDataTable = new DataTable();
            SqlConnection sqlConnection = new SqlConnection(sConStringDownLoad);
            sqlConnection.Open();
            SqlDataAdapter sqlOilAdapter = new SqlDataAdapter(sGetTables, sqlConnection);
            sqlOilAdapter.Fill(dtTableDataTable);
            sqlConnection.Close();

            foreach(DataRow drTable in dtTableDataTable.Rows)
            {
                string sTableName = drTable["TableName"].ToString();
                if (!lstTableNames.Contains(sTableName))
                {
                    lstTableNames.Add(sTableName);

                }
            }
        }
        /// <summary>
        /// This will download and pass back a single table based on your table names
        /// </summary>
        /// <param name="sTableName"></param>
        /// <returns></returns>
        public DataTable GetSingleTable(string sTableName)
        {
            DataTable dtTabletoReturn = new DataTable();
            string sCommand = "SELECT * FROM " + sTableName;
            SqlConnection sqlConnection = new SqlConnection(sConStringDownLoad);
            sqlConnection.Open();
            SqlDataAdapter sqlOilAdapter = new SqlDataAdapter(sCommand, sqlConnection);
            sqlOilAdapter.Fill(dtTabletoReturn);
            sqlConnection.Close();
            return dtTabletoReturn;
        }
        /// <summary>
        /// This will upload a table to the destination Database using a constring provided.
        /// Take note the source table and destination table need to be the same in schema, you cannot upload a table with a different structure.
        /// </summary>
        /// <param name="sDestinationTable"></param>
        /// <param name="dtTableToUpload"></param>
        /// <param name="sConnString"></param>
        /// <returns></returns>
        public bool SuperBulkUploadTable(string sDestinationTable, DataTable dtTableToUpload, string sConnString) // Only use when ulpoading a table with the same columns as dest table
        {
            bool isUploaded = true;
            try
            {
                SqlConnection sqlConn = new SqlConnection(sConnString);
                SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sqlConn);
                sqlBulkCopy.DestinationTableName = sDestinationTable;
                foreach (DataColumn dcUploadColumn in dtTableToUpload.Columns)
                {
                    sqlBulkCopy.ColumnMappings.Add(dcUploadColumn.ColumnName, dcUploadColumn.ColumnName);
                }
                sqlConn.Open();
                sqlBulkCopy.WriteToServer(dtTableToUpload);
                sqlConn.Close();
                isUploaded = true;
            }
            catch
            {
                isUploaded = false;
            }
            return isUploaded;
        }
    }
}