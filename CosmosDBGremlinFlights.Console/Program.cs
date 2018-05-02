using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CsvHelper;
using GeoCoordinatePortable;
using Gremlin.Net.Driver;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;
using Polly;

namespace CosmosDBGremlinFlights.Console
{
    class Program
    {
        // Azure Cosmos DB Configuration variables
        // Replace the values in these variables to your own.
        // Here database = "flightsdb" & collection = "flights" are used by default
        // in reference to Anthony Chu's original version
        private static string hostname = "your-endpoint.gremlin.cosmosdb.azure.com";
        private static int port = 443;
        private static string authKey = "your-authentication-key";
        private static string database = "flightsdb";
        private static string collection = "flights";

        private static readonly HttpClient client = new HttpClient();

        static void Main(string[] args)
        {            
            LoadAirports().Wait();
        }

         private static async Task LoadAirports()
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .RetryAsync(5);

            var airports = new Dictionary<string, Airport>();
            var routes = new HashSet<Tuple<string, string>>();   

             using (var httpClient = new HttpClient())
             {
                 var airportsCsvStream = await httpClient.GetStreamAsync("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat");

                 var gremlinServer = new GremlinServer(hostname, port, enableSsl: true,
                                                    username: "/dbs/" + database + "/colls/" + collection,
                                                    password: authKey);

                 using (var fileStream = new FileStream("airports.dat", FileMode.Create, FileAccess.Write))
                 {
                     await airportsCsvStream.CopyToAsync(fileStream);
                     airportsCsvStream.Close();
                 }

                 using (var fileStream = new FileStream("airports.dat", FileMode.Open))
                 using (var reader = new CsvReader(new StreamReader(fileStream)))
                 {
                    var count = 0;
                    while (reader.Read())
                    {                        
                        var name = reader.GetField(1);
                        var code = reader.GetField(4);
                        var lat = reader.GetField(6);
                        var lng = reader.GetField(7);

                        if (!new string[] { @"\N", "N/A", "" }.Contains(code))
                        { 
                            reader.Configuration.BadDataFound = null;                         
                            var gremlinQuery = $"g.addV('airport').property('id', \"{code}\").property('latitude', {lat}).property('longitude', {lng})";
                           
                            var airport = new Airport
                            {
                                Code = code,
                                Name = name,
                                Coordinate = new GeoCoordinate(Convert.ToDouble(lat), Convert.ToDouble(lng))
                            };
                            airports.Add(code, airport);                            
                            
                            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
                            {
                                var task = gremlinClient.SubmitAsync<dynamic>(gremlinQuery);
                                count++;

                                foreach (var result in task.Result){
                                    string output = JsonConvert.SerializeObject(result);
                                    System.Console.WriteLine($"{count} {output}");                                    
                                }                              

                            }                            
                        }
                    }
                }
                
                var routesCsvStream = await httpClient.GetStreamAsync("https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat");
                using (var reader = new CsvReader(new StreamReader(routesCsvStream)))
                {
                    var count = 0;
                    while (reader.Read())
                    {
                        var airline = reader.GetField(0);
                        var from = reader.GetField(2);
                        var to = reader.GetField(4);
                        var stops = reader.GetField(7);

                        if (string.IsNullOrEmpty(from) || string.IsNullOrEmpty(to))
                        {
                            continue;
                        }

                        var route = Tuple.Create(from, to);

                        airports.TryGetValue(from, out var fromAirport);
                        airports.TryGetValue(to, out var toAirport);

                        var isDirect = stops == "0";

                        if (isDirect && !routes.Contains(route) && fromAirport != null && toAirport != null)
                        {
                            routes.Add(route);
                            var distance = fromAirport.Coordinate.GetDistanceTo(toAirport.Coordinate);
                            var gremlinQuery = $"g.V('{fromAirport.Code}').addE('flight').to(g.V('{toAirport.Code}')).property('distance', {distance})";
                            
                            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
                            {
                                var task = gremlinClient.SubmitAsync<dynamic>(gremlinQuery);
                                count++;

                                foreach (var result in task.Result){
                                    string output = JsonConvert.SerializeObject(result);
                                    System.Console.WriteLine($"{count} {output}");                                    
                                }                              

                            }  
                        }
                    }
                }
             }               
        }        
    }
}
