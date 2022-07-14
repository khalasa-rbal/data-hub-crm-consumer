//
// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//

//using Amazon.Kinesis.Model;
//using DocumentFormat.OpenXml.Math;
//using Newtonsoft.Json;
//using Amazon.Kinesis.ClientLibrary.SampleConsumer;
using System;
using System.Collections.Generic;
using System.IO;
//using System.Diagnostics;
//using System.IO;
//using System.Text;
using System.Threading;
//using System.Threading.Tasks;



namespace Amazon.Kinesis.ClientLibrary.SampleConsumer
{
    /// A sample processor of Kinesis records.

    internal class SampleRecordProcessor : IShardRecordProcessor
    {
        /// <value>The time to wait before this record processor
        /// reattempts either a checkpoint, or the processing of a record.</value>
        private static readonly TimeSpan Backoff = TimeSpan.FromSeconds(3);

        /// <value>The interval this record processor waits between
        /// doing two successive checkpoints.</value>
        private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMinutes(1);

        /// <value>The maximum number of times this record processor retries either
        /// a failed checkpoint, or the processing of a record that previously failed.</value>
        private static readonly int NumRetries = 10;

        /// <value>The shard ID on which this record processor is working.</value>
        private string _kinesisShardId;

        /// <value>The next checkpoint time expressed in milliseconds.</value>
        private DateTime _nextCheckpointTime = DateTime.UtcNow;

        /// <summary>
        /// This method is invoked by the Amazon Kinesis Client Library before records from the specified shard
        /// are delivered to this SampleRecordProcessor.
        /// </summary>
        /// <param name="input">
        /// InitializationInput containing information such as the name of the shard whose records this
        /// SampleRecordProcessor will process.
        /// </param>
        public void Initialize(InitializationInput input)
        {
            Console.Error.WriteLine("Initializing record processor for shard: " + input.ShardId);
            _kinesisShardId = input.ShardId;
        }

        /// <summary>
        /// This method processes the given records and checkpoints using the given checkpointer.
        /// </summary>
        /// <param name="input">
        /// ProcessRecordsInput that contains records, a Checkpointer and contextual information.
        /// </param>
        public void ProcessRecords(ProcessRecordsInput input)
        {
            // Process records and perform all exception handling.
            ProcessRecordsWithRetries(input.Records);

            // Checkpoint once every checkpoint interval.
            if (DateTime.UtcNow >= _nextCheckpointTime)
            {
                Checkpoint(input.Checkpointer);
                _nextCheckpointTime = DateTime.UtcNow + CheckpointInterval;
            }
        }

        /// <summary>
        /// This method processes records, performing retries as needed.
        /// </summary>
        /// <param name="records">The records to be processed.</param>
        private static void ProcessRecordsWithRetries(List<Record> records)
        {
            foreach (Record rec in records)
            {
                bool processedSuccessfully = false;
                string data = null;
                for (int i = 0; i < NumRetries; ++i)
                {
                    try
                    {
                        // As per the accompanying AmazonKinesisSampleProducer.cs, the payload
                        // is interpreted as UTF-8 characters.
                        data = System.Text.Encoding.UTF8.GetString(rec.Data);

                        // Uncomment the following if you wish to see the retrieved record data.
                        //Console.Error.WriteLine(
                        //    String.Format("Retrieved record:\n\tpartition key = {0},\n\tsequence number = {1},\n\tdata = {2}",
                        //    rec.PartitionKey, rec.SequenceNumber, data));

                        // Your own logic to process a record goes here.

                        processedSuccessfully = true;
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.Error.WriteLine("Exception processing record data: " + data, e);
                        //Back off before retrying upon an exception.
                        Thread.Sleep(Backoff);
                    }
                }

                if (!processedSuccessfully)
                {
                    Console.Error.WriteLine("Couldn't process record " + rec + ". Skipping the record.");
                }
            }
        }

        /// <summary>
        /// This checkpoints the specified checkpointer with retries.
        /// </summary>
        /// <param name="checkpointer">The checkpointer used to do checkpoints.</param>
        private void Checkpoint(Checkpointer checkpointer)
        {
            Console.Error.WriteLine("Checkpointing shard " + _kinesisShardId);

            // You can optionally provide an error handling delegate to be invoked when checkpointing fails.
            // The library comes with a default implementation that retries for a number of times with a fixed
            // delay between each attempt. If you do not provide an error handler, the checkpointing operation
            // will not be retried, but processing will continue.
            checkpointer.Checkpoint(RetryingCheckpointErrorHandler.Create(NumRetries, Backoff));
        }

        public void LeaseLost(LeaseLossInput leaseLossInput)
        {
            //
            // Perform any necessary cleanup after losing your lease.  Checkpointing is not possible at this point.
            //
            Console.Error.WriteLine($"Lost lease on {_kinesisShardId}");
        }

        public void ShardEnded(ShardEndedInput shardEndedInput)
        {
            //
            // Once the shard has ended it means you have processed all records on the shard. To confirm completion the
            // KCL requires that you checkpoint one final time using the default checkpoint value.
            //
            Console.Error.WriteLine(
                $"All records for {_kinesisShardId} have been processed, starting final checkpoint");
            shardEndedInput.Checkpointer.Checkpoint();
        }

        public void ShutdownRequested(ShutdownRequestedInput shutdownRequestedInput)
        {
            Console.Error.WriteLine($"Shutdown has been requested for {_kinesisShardId}. Checkpointing");
            shutdownRequestedInput.Checkpointer.Checkpoint();
        }
    }

    internal class MainClassBase
    {
        private readonly object ACCESS_KEY;
        private readonly object SECRET_KEY;

        public void ViewDidLoad()
        {
            //Base.ViewDidLoad();
            AmazonKinesisClient client = new(BasicAWSCredentials(ACCESS_KEY, SECRET_KEY), RegionEndpoint.EUCentral1);
            //AmazonKinesisClient client = new AmazonKinesisClient(BasicAWSCredentials(ACCESS_KEY, SECRET_KEY), RegionEndpoint.EUCentral1);

            object putResponse = PutRecord(client);

            var response = GetRecords(client, putResponse.ShardId);

            for (int i = 0; i < response.Records.Count; ++i)
            {
                Console.WriteLine("Record: " + response.Records[i].Data.ReadByte());
            }
        }
        private static RegionEndpoint BasicAWSCredentials(object aCCESS_KEY, object sECRET_KEY)
        {
            throw new NotImplementedException();
        }

        private static GetRecordsResponse GetRecords(AmazonKinesisClient client, string shardId)
        {
            var siRequest = new GetShardIteratorRequest
            {
                ShardId = shardId,
                StreamName = "Test1",
                ShardIteratorType = "TRIM_HORIZON"
            };

            var siResponse = client.GetShardIteratorRequest(siRequest);
            var request = new GetRecordsRequest
            {
                ShardIterator = siResponse.ShardIterator
            };
            return client.GetRecords(request);
        }
        private object GetRecords(AmazonKinesisClient client, object shardId)
        {
            throw new NotImplementedException();
        }

        private object PutRecord(AmazonKinesisClient client)
        {
            throw new NotImplementedException();
        }

        private class GetShardIteratorRequest
        {
            public string ShardId { get; set; }
            public string StreamName { get; set; }
            public string ShardIteratorType { get; set; }

        }
    }

    internal class AmazonKinesisClient
    {
        private readonly RegionEndpoint regionEndpoint;
        private readonly RegionEndpoint eUCentral1;

        public AmazonKinesisClient(AmazonKinesisConfig config)
        {
        }

        public AmazonKinesisClient(RegionEndpoint regionEndpoint, RegionEndpoint eUCentral1)
        {
            this.regionEndpoint = regionEndpoint;
            this.eUCentral1 = eUCentral1;
        }

        internal GetRecordsResponse GetRecords(GetRecordsRequest request)
        {
            throw new NotImplementedException();
        }
        internal object GetShardIterator(GetShardIteratorRequest siRequest)
        {
            throw new NotImplementedException();
        }
        internal object PutRecordAsync(PutRecordRequest requestRecord)
        {
            throw new NotImplementedException();
        }

    }

    internal class GetShardIteratorRequest
    {
        internal object ShardIterator;
    }

    internal class GetRecordsRequest
    {
        internal object ShardIterator;
    }
    internal class PutRecordRequest
    {
        //internal string RecordId { get; set; }
        internal string StreamName;
        internal string PartitionKey;
        internal string ShardId;
        internal MemoryStream Data;
    }

}