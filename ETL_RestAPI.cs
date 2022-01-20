using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net;
using System.Net.Http.Headers;
using System.Collections.Generic;

namespace Company.Function
{

    public static class ETL_RestAPI
    {
        [FunctionName("ETL_RestAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("Called ETL_RESTAPI... ");

            try
            {
                // Call Your  API
                HttpClient client = new HttpClient();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                // string apiKey = Environment.GetEnvironmentVariable("apiKey");
                string apiCallQuery = "https://api.trello.com/1/boards/i8UbFIV7/lists?key=" + Environment.GetEnvironmentVariable("apiKey");

                // List data response.
                HttpResponseMessage response = client.GetAsync(apiCallQuery).Result;  // Blocking call! Program will wait here until a response is received or a timeout occurs.
                if (response.IsSuccessStatusCode)
                {
                    // Parse the response body.
                    var dataObjects = response.Content.ReadAsStringAsync().Result;  //Make sure to add a reference to System.Net.Http.Formatting.dll
                    return new OkObjectResult(dataObjects);
                }
                else
                {
                    Console.WriteLine("{0} ({1})", (int)response.StatusCode, response.ReasonPhrase);
                }
                client.Dispose();
                return new OkObjectResult("End");
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.ToString());
                return new OkObjectResult("Error");
            }
        }
    }
}
