using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Azure.Services.AppAuthentication;
using System.Threading.Tasks;

namespace ETL
{
    public static class ETL_Insight
    {
        [FunctionName("ETL_Insight")]
        public static async Task Run([TimerTrigger("0 30 * * * *")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            var tokenProvider = new AzureServiceTokenProvider();
            string accessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");

            string[] entities = new string[] { "region", "company", "product", "productFamily", "productLine", "platform", "productVersion", "productVersionPlatform", "contact" };
            foreach (string entity in entities)
            {
                string sqlPath = Path.Combine(context.FunctionAppDirectory, "sql", $"{entity}.sql");
                string sqlQuery = File.ReadAllText(sqlPath);
                log.LogInformation($"Start ETL process for entity: {entity}");
                try
                {
                    long lastUpdate = getLastUpdateOfEntity(entity, accessToken);
                    log.LogInformation($"Last update of {entity}: {lastUpdate}");
                    DataTable dataFromSource = extractDataFromSource(entity, sqlQuery, lastUpdate);
                    log.LogInformation($"Extracted data for {entity}");
                    loadDataToDWH(dataFromSource, entity, accessToken);
                    log.LogInformation($"Completed ETL for {entity}");
                }
                catch (SqlException ex)
                {
                    log.LogError(ex, $"ERROR execution ETL for {entity}; ExceptionMessage: ${ex.ToString()}");
                }
            }
        }

        public static long getLastUpdateOfEntity(string tableName, string accessToken)
        {
            DateTime lastUpdateDateTime;
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("connectionstringDWH")))
            {
                conn.AccessToken = accessToken;
                conn.Open();
                var sqlQuery = $"SELECT MAX(UPDATED) FROM {tableName}";
                using (SqlCommand cmd = new SqlCommand(sqlQuery, conn))
                {
                    object lastUpdateObj = cmd.ExecuteScalar();
                    if (lastUpdateObj == null || lastUpdateObj == DBNull.Value) { return 0; }
                    lastUpdateDateTime = (DateTime)lastUpdateObj;
                }
            }
            TimeSpan diffToOriginTime = lastUpdateDateTime.ToUniversalTime() - DateTime.UnixEpoch;
            long lastUpdate = ((long)diffToOriginTime.TotalSeconds);
            return lastUpdate;
        }

        public static DataTable extractDataFromSource(string entity, string sqlQuery, long lastUpdate)
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
                conn.AccessToken = accessToken;
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