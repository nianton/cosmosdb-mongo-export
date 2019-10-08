using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CosmosDbExport
{
    public static class ExportFunction
    {
        /// <summary>
        /// The number of days from which older records will be exported.
        /// </summary>
        private static int NumberOfDaysThreshold = 180;

        private const string ThrottlingErrorMessage = "request rate is large";
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private static readonly string CosmosDbConnectionString;
        private static readonly string CosmosDbName;
        private static readonly string CosmosDbCollectionName;
        private static readonly string StorageConnectionString;
        private static readonly string ExportContainerName;
        private static readonly CloudStorageAccount StorageAccount;
        private static readonly CloudBlobClient BlobClient;

        /// <summary>
        /// Initialize configuration for this function.
        /// </summary>
        static ExportFunction()
        {
            NumberOfDaysThreshold = int.Parse(Environment.GetEnvironmentVariable("NumberOfDaysThreshold"));
            CosmosDbConnectionString = Environment.GetEnvironmentVariable("CosmosDbConnectionString");
            CosmosDbName = Environment.GetEnvironmentVariable("CosmosDbName");
            CosmosDbCollectionName = Environment.GetEnvironmentVariable("CosmosDbCollectionName");
            StorageConnectionString = Environment.GetEnvironmentVariable("StorageConnectionString");
            ExportContainerName = Environment.GetEnvironmentVariable("ExportContainerName");
            StorageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            BlobClient = StorageAccount.CreateCloudBlobClient();
        }

        /// <summary>
        /// Actual export method, runs on a schedule
        /// </summary>
        /// <param name="myTimer"></param>
        /// <param name="log"></param>
        [FunctionName(nameof(ExportFunction))]
        public static async Task Run([TimerTrigger("0 */30 * * * *", RunOnStartup = true)]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var mongoClientSettings = MongoClientSettings.FromUrl(new MongoUrl(CosmosDbConnectionString));
            var mongoClient = new MongoClient(mongoClientSettings);
            var mongoDatabase = mongoClient.GetDatabase(CosmosDbName);
            var mongoCollection = mongoDatabase.GetCollection<BsonDocument>(CosmosDbCollectionName);

            var filter = GetDocumentExportFilter();
            var options = new FindOptions<BsonDocument>
            {
                BatchSize = 20,
                NoCursorTimeout = false
            };

            var blobContainer = BlobClient.GetContainerReference(ExportContainerName);
            var cursor = await mongoCollection.FindAsync(filter, options);
            try
            {
                while (await cursor.MoveNextAsync())
                {
                    IEnumerable<BsonDocument> batch = cursor.Current;
                    foreach (var record in batch)
                    {
                        var recordJson = record.ToJson();
                        var recordId = record.GetValue("_id").AsString;
                        var recordDate = UnixEpoch.AddMilliseconds(record.GetValue("_created_at").AsBsonDateTime.MillisecondsSinceEpoch);
                        var blob = blobContainer.GetBlockBlobReference($"{CosmosDbCollectionName}/{recordDate:yyyyMMddHHmmss}_{recordId}.json");
                        await blob.UploadTextAsync(recordJson);

                        var deleteDocumentElements = new[] {
                            new BsonElement("_id", recordId)
                        }.ToList();
                        var deleteResult = await mongoCollection.DeleteOneAsync(new BsonDocument(deleteDocumentElements));
                    }
                }
            }
            catch (Exception ex)
            {
                if (!IsThrottled(ex))
                {
                    log.LogError(ex, "ERROR: With collection {0}", ex.ToString());
                    throw;
                }
                else
                {
                    // Thread will wait in between 1.5 secs and 3 secs.
                    await Task.Delay(new Random().Next(1500, 3000));
                }
            }
        }

        /// <summary>
        /// Setup the filter criteria for the documents to be deleted.
        /// </summary>
        /// <returns></returns>
        private static FilterDefinition<BsonDocument> GetDocumentExportFilter()
        {
            var dateThreshold = DateTime.UtcNow.Subtract(TimeSpan.FromDays(NumberOfDaysThreshold));

            var builder = Builders<BsonDocument>.Filter;
            var filter = builder.Lt("_created_at", new BsonDateTime(dateThreshold));

            return filter;
        }

        /// <summary>
        /// Returns whether the exception is caused by a throttling response by CosmosDb.
        /// </summary>
        /// <param name="ex"></param>
        private static bool IsThrottled(Exception ex)
        {
            return ex.Message.ToLower().Contains(ThrottlingErrorMessage);
        }
    }
}
