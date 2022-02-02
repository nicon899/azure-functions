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
    public static class ETL_JSM
    {
        [FunctionName("ETL_JSM")]
        public static async Task Run([TimerTrigger("0 20 * * * *")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            var tokenProvider = new AzureServiceTokenProvider();
            string accessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");
            string entity = "issue";
            string sqlPath = Path.Combine(context.FunctionAppDirectory, "sql", $"{entity}.sql");
            string sqlQuery = File.ReadAllText(sqlPath);
            log.LogInformation($"Start ETL process for entity: {entity}");
            try
            {
                DateTime lastUpdate = getLastUpdateOfEntity(entity, accessToken);
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

        public static DateTime getLastUpdateOfEntity(string tableName, string accessToken)
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
                    cmd.Parameters.AddWithValue("@lastUpdate", lastUpdate.ToString());
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

                // create temp table
                string sqlCreateTmpTable = $"SELECT TOP 0 * INTO #tmp{tableName} FROM {tableName}";
                using (SqlCommand cmd = new SqlCommand(sqlCreateTmpTable, conn))
                {
                    cmd.CommandTimeout = 3600;
                    cmd.ExecuteNonQuery();
                }

                // bulk copy data into temp table
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = $"#tmp{tableName}";
                    bulkCopy.WriteToServer(data);
                }

                // update table
                string sqlUpdateTable = $"BEGIN TRANSACTION; DELETE FROM {tableName} WHERE IssueID IN (SELECT IssueID FROM #tmp{tableName}); INSERT INTO {tableName} SELECT * FROM #tmp{tableName}; COMMIT TRANSACTION; DROP TABLE #tmp{tableName}";
                using (SqlCommand cmd = new SqlCommand(sqlUpdateTable, conn))
                {
                    cmd.CommandTimeout = 3600;
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}