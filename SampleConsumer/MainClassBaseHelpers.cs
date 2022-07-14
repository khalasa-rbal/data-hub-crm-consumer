using Amazon.Kinesis;
using Amazon.KinesisFirehose.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
//
// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
//
namespace Amazon.Kinesis.ClientLibrary.SampleConsumer
{
    internal static class MainClassBaseHelpers
    {

        /// <summary>
        /// This method creates a KclProcess and starts running an SampleRecordProcessor instance.
        /// </summary>
        public static void Main(string[] args)
        {
            try
            {
                KclProcess.Create(new SampleRecordProcessor()).Run();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("ERROR: " + e);
            }
        }

        private static Amazon.RegionEndpoint BasicAWSCredentials(object aCCESS_KEY, object sECRET_KEY)
        {
            throw new NotImplementedException();
        }
        private static void Main(string[] args, ConfigurationManager configurationManager, string ms)
        {
            var o = new
            {
                Message = "Hello World",
                Author = "Krista Ifti"
            };

            //convert to byte array in prep for adding to stream
            byte[] oByte = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(o));

            //create config that points to AWS region

            var config = new AmazonKinesisConfig
            {
                RegionEndpoint = RegionEndpoint.EUCentral1
            };

            //create client that pulls creds from web.config and takes in Kinesis config
            var client = new AmazonKinesisClient(config);

            var sw = Stopwatch.StartNew();
            //Stopwatch sw2 = null;

            var tasks = new List<Task<PutRecordResponse>>();

            ConfigurationManager configurationManager1 = configurationManager;
            var count = int.Parse(configurationManager1.AppSettings["Count"]);
            Console.WriteLine("Sending {0} records... One at a time...", count);
            sw.Restart();
            for (int i = 0; i < count; i++)
            {
                NewMethod(oByte, client, tasks, i);
                //sw2.Stop();
                ///Console.WriteLine("Async latency is {0}", sw2.ElapsedMilliseconds);
            }

            Console.WriteLine("{0} records sent... Waiting for tasks to complete...", count);
            Task.WaitAll(tasks.ToArray(), -1);
            sw.Stop();
            foreach (var t in tasks)
            {
                if (t.Result.HttpStatusCode != System.Net.HttpStatusCode.OK)
                {
                    Console.WriteLine(t.Result.HttpStatusCode);
                }
            }
            /*  double actionsPerSec = (double)count * 1000 / (double)sw.ElapsedMilliseconds;
              Console.WriteLine("{0} requests in {1} ms. {2:0.00} requests/sec.", count, sw.ElapsedMilliseconds, actionsPerSec);*/
            double actionsPerSec = (double)count * 1000 / sw.ElapsedMilliseconds;
            Console.WriteLine("{0} requests in {1} ms. {2:0.00} requests/sec.", count, sw.ElapsedMilliseconds, actionsPerSec);
        }
        private static void NewMethod(byte[] oByte, AmazonKinesisClient client, List<Task<PutRecordResponse>> tasks, int i)
        {
            //System.Threading.Thread.Sleep(10);
            //sw2 = Stopwatch.StartNew();
            //create stream object to add to Kinesis request
            using MemoryStream ms = new(oByte);
            //create put request
            PutRecordRequest requestRecord = new();
            //list name of Kinesis stream
            requestRecord.StreamName = "shomi_dev";
            //give partition key that is used to place record in particular shard
            requestRecord.PartitionKey = i.ToString();
            //add record as memorystream
            requestRecord.Data = ms;

            //PUT the record to Kinesis
            var task = client.PutRecordAsync(requestRecord);
            tasks.Add((Task<PutRecordResponse>)task);
        }

        internal class ConfigurationManager
        {
            public static object AppSettings { get; internal set; }

        }
    }

    internal class AmazonKinesisConfig
    {
        public RegionEndpoint RegionEndpoint { get; set; }
    }
}