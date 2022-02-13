using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Azure.Services.AppAuthentication;

namespace ETL_HTTP
{
    public static class ETL_HTTPTriggerTranTest
    {
        [FunctionName("ETL_HTTPTriggerTranTest")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {
            var tokenProvider = new AzureServiceTokenProvider();
            string accessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");

            string[] entities = new string[] { "regiondev" };
            foreach (string entity in entities)
            {
                log.LogInformation($"Start ETL process for entity: {entity}");
                try
                {
                    DateTime lastUpdate = getLastUpdateOfEntity(entity, accessToken);
                    log.LogInformation($"Last update: {lastUpdate.ToString()}");
                    string sqlPath = Path.Combine(context.FunctionAppDirectory, "sql", $"{entity}.sql");
                    string sqlQuery = File.ReadAllText(sqlPath);
                    DataTable dataFromSource = extractDataFromSource(entity, sqlQuery, lastUpdate);
                    log.LogInformation($"Extracted Data from source");
                    loadDataToDWH(dataFromSource, entity, accessToken);
                    log.LogInformation($"Completed ETL for {entity}");
                }
                catch (SqlException ex)
                {
                    log.LogError(ex, $"ERROR execution ETL for {entity}; ExceptionMessage: ${ex.ToString()}");
                }
            }
            return new OkObjectResult("responseMessage");
        }

        public static DateTime getLastUpdateOfEntity(string tableName, string accessToken)
        {
            DateTime lastUpdateDateTime;
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("connectionstringDWH")))
            {
                // conn.AccessToken = accessToken;
                conn.Open();
                var sqlQuery = $"SELECT MAX(UPDATED) FROM {tableName}";
                using (SqlCommand cmd = new SqlCommand(sqlQuery, conn))
                {
                    object lastUpdateObj = cmd.ExecuteScalar();
                    DateTime myDate = DateTime.ParseExact("2009-05-08 14:40:52,531", "yyyy-MM-dd HH:mm:ss,fff",
                                       System.Globalization.CultureInfo.InvariantCulture);
                    if (lastUpdateObj == null || lastUpdateObj == DBNull.Value) { return myDate; }
                    lastUpdateDateTime = (DateTime)lastUpdateObj;
                    return lastUpdateDateTime;
                }
            }
        }

        public static DataTable extractDataFromSource(string entity, string sqlQuery, DateTime lastUpdate)
        {
            DataTable dataFromSource = new DataTable();
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("connectionstringJSM")))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sqlQuery, conn))
                {
                    cmd.CommandTimeout = 3600;
                    cmd.Prepare();
                    cmd.Parameters.AddWithValue("@lastUpdate", lastUpdate);
                    using (SqlDataAdapter sda = new SqlDataAdapter(cmd))
                    {
                        sda.Fill(dataFromSource);
                    }
                }
            }
            return dataFromSource;
        }

        public static void loadDataToDWH(DataTable data, string tableName, string accessToken)
        {
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("connectionstringDWH")))
            {
                //  conn.AccessToken = accessToken;
                conn.Open();
                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    // create temp table
                    Console.WriteLine("Create tmp table");
                    string sqlCreateTmpTable = $"SELECT TOP 0 * INTO #tmp{tableName} FROM {tableName}";
                    using (SqlCommand cmd = new SqlCommand(sqlCreateTmpTable, conn, transaction))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    // bulk copy data into temp table
                    Console.WriteLine("Bulk copy starting...");
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = $"#tmp{tableName}";
                        bulkCopy.WriteToServer(data);
                    }
                    // update table
                    Console.WriteLine("Update");
                    string sqlUpdateTable = $"DELETE FROM {tableName} WHERE ID IN (SELECT ID FROM #tmp{tableName});"
                                                + $"INSERT INTO {tableName} SELECT * FROM #tmp{tableName};"
                                                + $"DROP TABLE #tmp{tableName};";
                    using (SqlCommand cmd = new SqlCommand(sqlUpdateTable, conn, transaction))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

    }
}