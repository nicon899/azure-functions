using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Microsoft.Azure.Services.AppAuthentication;
using Dapper;

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Data.SqlClient.AlwaysEncrypted.AzureKeyVaultProvider;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.SqlServer.Management.AlwaysEncrypted.AzureKeyVaultProvider;

namespace ETL
{
    public static class ETL_Insight_Test
    {
        [FunctionName("ETL_Insight_Test")]
        public static async Task Run([TimerTrigger("0 */3 * * * *")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            var tokenProvider = new AzureServiceTokenProvider();
            string accessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");

            string[] entities = new string[] { "region" }; // , "company", "product", "productFamily", "productLine", "platform", "productVersion", "productVersionPlatform", "contact"
            foreach (string entity in entities)
            {
                string sqlPath = Path.Combine(context.FunctionAppDirectory, "sql", $"{entity}.sql");
                string sqlQuery = File.ReadAllText(sqlPath);
                log.LogInformation($"Start ETL process for entity: {entity}");
                try
                {
                    long lastUpdate = getLastUpdateOfEntity(entity, accessToken);
                    log.LogInformation($"Last update of {entity}: {lastUpdate}");
                    DataTable dataFromSource = extractDataFromSource(entity, sqlQuery, lastUpdate, accessToken);
                    log.LogInformation($"Extracted data for {entity}");
                    loadDataToDWH(dataFromSource, entity, accessToken, log);
                    log.LogInformation($"Completed ETL for {entity}");
                }
                catch (Exception ex)
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
                using (SqlCommand cmd = new SqlCommand(sqlQuery))
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

        public static DataTable extractDataFromSource(string entity, string sqlQuery, long lastUpdate, string accessToken)
        {
            DataTable dataFromSource = new DataTable();
            using (var conn = new SqlConnection(Environment.GetEnvironmentVariable("connectionstringJSM")))
            {
                conn.AccessToken = accessToken;
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(sqlQuery))
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

        public static void loadDataToDWH(DataTable data, string tableName, string accessToken, ILogger log)
        {
            ClientCredential _clientCredential = new ClientCredential('d80fcd73-493e-4aa1-a9f8-8ac9c4037b18', 'ul17Q~vu8J.iH7ivp72g168ro.v~X6~oOgvO8');
            Microsoft.Data.SqlClient.AlwaysEncrypted.AzureKeyVaultProvider.SqlColumnEncryptionAzureKeyVaultProvider azureKeyVaultProvider = new SqlColumnEncryptionAzureKeyVaultProvider(GetToken);
            Dictionary<string, SqlColumnEncryptionKeyStoreProvider> providers = new Dictionary<string, SqlColumnEncryptionKeyStoreProvider>();


            providers.Add(SqlColumnEncryptionAzureKeyVaultProvider.ProviderName, azureKeyVaultProvider);
            SqlConnection.RegisterColumnEncryptionKeyStoreProviders(providers);




            Map<String, SQLServerColumnEncryptionKeyStoreProvider> keyStoreMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
            keyStoreMap.put(akvProvider.getName(), akvProvider);
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(keyStoreMap);

            string execSql = @"INSERT INTO dbo.region VALUES (@id, @idKey, @name, GETDATE(), GETDATE());";
            DynamicParameters dp = new DynamicParameters();
            dp.Add("@id", 100, dbType: DbType.Int64, direction: ParameterDirection.Input, size: 100);
            dp.Add("@idKey", "13402871524", dbType: DbType.String, direction: ParameterDirection.Input, size: 100);
            dp.Add("@name", "CustomerA", dbType: DbType.String, direction: ParameterDirection.Input, size: 100);
            using (var conn = new SqlConnection(Environment.GetEnvironmentVariable("connectionstringDWH")))
            {
                conn.AccessToken = accessToken;
                conn.Open();
                conn.Execute(execSql, dp);
            }
        }


        public async static Task<string> GetToken(string authority, string resource, string scope)
        {
            var authContext = new AuthenticationContext(authority);
            AuthenticationResult result = await authContext.AcquireTokenAsync(resource, DefaultAuthContec);

            if (result == null)
                throw new InvalidOperationException("Failed to obtain the access token");
            return result.AccessToken;
        }
    }
}