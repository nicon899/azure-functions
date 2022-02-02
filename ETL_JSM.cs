using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace ETL
{
    public static class ETL_JSM
    {
        [FunctionName("ETL_JSM")]
         public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {
            var tokenProvider = new AzureServiceTokenProvider();
            string accessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");

            string[] entities = new string[] { "company", "contact", "platform", "productFamily", "productLine", "productVersionPlatform", "productVersion", "region" };
            foreach (string entity in entities)
            {
                string sqlPath = Path.Combine(context.FunctionAppDirectory, "sql", $"{entity}.sql");
                string sqlQuery = File.ReadAllText(sqlPath);
                log.LogInformation($"Start ETL process for entity: {entity}");
                DateTime lastUpdate;
                try
                {
                    lastUpdate = getLastUpdateOfEntity(entity, accessToken);
                    log.LogInformation($"Last update of {entity}: {lastUpdate}");
                    DataTable dataFromSource = extractDataFromSource(entity, sqlQuery, lastUpdate);
                    log.LogInformation($"Extracted data for {entity}");
                    loadDataToDWH(dataFromSource, entity, accessToken);
                    log.LogInformation($"Completed ETL for {entity}");
                }
                catch (SqlException ex)
                {
                    log.LogError(ex, $"ERROR execution ETL for {entity}");
                }
            }
            return new OkObjectResult("Response");
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
                    lastUpdateDateTime = (DateTime) lastUpdateObj;
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
                    cmd.ExecuteNonQuery();
                }

                // bulk copy data into temp table
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = $"#tmp{tableName}";
                    bulkCopy.WriteToServer(data);
                }

                // update table
                string sqlUpdateTable = $"BEGIN TRANSACTION; DELETE FROM {tableName} WHERE ID IN (SELECT ID FROM #tmp{tableName}); INSERT INTO {tableName} SELECT * FROM #tmp{tableName}; COMMIT TRANSACTION; DROP TABLE #tmp{tableName}";
                using (SqlCommand cmd = new SqlCommand(sqlUpdateTable, conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }
}