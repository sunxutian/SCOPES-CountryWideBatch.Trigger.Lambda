using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;
using Amazon.SimpleNotificationService;
using Amazon.Batch;
using Microsoft.Extensions.Configuration;
using Amazon.Batch.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace SCOPES.Batch.LambdaTrigger
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        IConfiguration Config { get; set; }
        IAmazonSimpleNotificationService SNSClient { get; set; }
        IAmazonBatch BatchClient { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            Config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json").Build();
            S3Client = new AmazonS3Client();
            SNSClient = new AmazonSimpleNotificationServiceClient();
            BatchClient = new AmazonBatchClient();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client, IAmazonSimpleNotificationService snsClient)
        {
            this.S3Client = s3Client;
            this.SNSClient = snsClient;
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<Guid> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var (id, riskIds) = await ProcessS3File(evnt, context.Logger).ConfigureAwait(false);

            if (id == Guid.Empty)
            {
                context.Logger.LogLine("error processing s3 file");
                return Guid.Empty;
            }

            await NotifyStatus(id, context.Logger).ConfigureAwait(false);

            // start batch
            await ScheduleBatch(id, riskIds, context.Logger).ConfigureAwait(false);

            return id;
        }

        private async Task<(Guid, string)> ProcessS3File(S3Event evnt, ILambdaLogger logger)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return (Guid.Empty, null);
            }

            try
            {
                if (!s3Event.Object.Key.Contains(Config.GetSection("RiskIdFilePrefix").Value))
                {
                    logger.LogLine($"File {s3Event.Object.Key} from bucket {s3Event.Bucket.Name} is not an valid file");
                    return (Guid.Empty, null);
                }

                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                Guid id = Guid.NewGuid();
                if (response != null)
                {
                    string fileName = $@"PendingProcessing/{Path.GetFileNameWithoutExtension(s3Event.Object.Key)}_{id}"
                        + $"{Path.GetExtension(s3Event.Object.Key)}";

                    await S3Client.CopyObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key, s3Event.Bucket.Name, fileName)
                        .ConfigureAwait(false);

                    await S3Client.DeleteObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key).ConfigureAwait(false);

                    using (StreamReader reader = new StreamReader(
                        await S3Client.GetObjectStreamAsync(s3Event.Bucket.Name, fileName, null)))
                    {
                        var content = await reader.ReadToEndAsync();
                        return (id, content);
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogLine(e.Message);
                logger.LogLine(e.StackTrace);
                throw;
            }

            return (Guid.Empty, null);
        }

        private async Task<bool> NotifyStatus(Guid id, ILambdaLogger logger)
        {
            try
            {
                var topic = await SNSClient.FindTopicAsync(Config.GetSection("SimpleNotificationServiceTopic").Value);

                if (topic != null)
                {
                    await SNSClient.PublishAsync(topic.TopicArn, $"Batch job will be scheduled for {id}").ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                logger.LogLine("error on sending status");
                logger.LogLine(ex.Message);
                logger.LogLine(ex.StackTrace);
                throw;
            }
            
            return true;
        }

        private async Task<bool> ScheduleBatch(Guid id, string riskIds, ILambdaLogger logger)
        {
            try
            {
                // todo add batch
                SubmitJobRequest request = new SubmitJobRequest();
                request.Parameters = new Dictionary<string, string>
                {
                    {"fileId", id.ToString()}
                };

                //BatchClient.SubmitJobAsync()

                return true;
            }
            catch (Exception ex)
            {
                logger.LogLine("error on starting batch");
                logger.LogLine(ex.Message);
                logger.LogLine(ex.StackTrace);
                throw;
            }
        }
    }
}
