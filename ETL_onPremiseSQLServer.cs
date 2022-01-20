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

namespace Company.Function
{
    public static class HttpTrigger1
    {
        [FunctionName("ETL_onPremiseSQLServer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            DataTable dataFromSource = new DataTable();
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("source_connectionstring")))
            {
                conn.Open();
                var text = "SELECT *  FROM [dcmArchiv].[dbo].[dcm2016DevelDB_diveTable]";
                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    cmd.CommandType = CommandType.Text;
                    using (SqlDataAdapter sda = new SqlDataAdapter(cmd))
                    {
                        sda.Fill(dataFromSource);
                    }
                }
            }

             using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("dest_connectionstring")))
            {
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
