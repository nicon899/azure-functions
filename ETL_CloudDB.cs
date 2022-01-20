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
using Microsoft.Azure.Services.AppAuthentication;

namespace Company.Function
{
    public static class Test
    {
        [FunctionName("ETLCloudDB")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            DataTable dataFromSource = new DataTable();
            log.LogInformation("Connect to Source DB");
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("source_connectionstring")))
            {
                conn.Open();
                var text = "SELECT * FROM SalesLT.Customer";
                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    cmd.CommandType = CommandType.Text;
                    using (SqlDataAdapter sda = new SqlDataAdapter(cmd))
                    {
                        sda.Fill(dataFromSource);
                    }
                }
            }
            log.LogInformation("Loaded Data from SourceDB");
            var tokenProvider = new AzureServiceTokenProvider();
            string accessToken = await tokenProvider.GetAccessTokenAsync("https://database.windows.net/");
            log.LogInformation("Got Token for Dest DB");
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("dest_connstring")))
            {
                log.LogInformation("Connect to Dest DB");
                conn.AccessToken = accessToken;
                conn.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "dbo.Customer";
                    try
                    {
                        // Write unchanged rows from the source to the destination.
                        bulkCopy.WriteToServer(dataFromSource);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }

            return new OkObjectResult("Response");
        }
    }
}