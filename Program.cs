using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Moriyama.BlobCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            string sourceDirectory = RationalisePath(args[0]);
            string containerName = args[1];

            var uploads = new List<ManualResetEvent>();
            int started = 0;
            int completed = 0;
            
            foreach (string filePath in Directory.EnumerateFiles(sourceDirectory, "*", SearchOption.AllDirectories))
            {
                // Wait once number of threads is maxxed.
                if (uploads.Count >= 64)
                {
                    Console.WriteLine("Waiting");
                    var done = WaitHandle.WaitAny(uploads.ToArray());
                    uploads.RemoveAt(done);
                    completed++;

                    Console.WriteLine("Continuing: " + completed + " uploads completed");
                }

                var resetEvent = new ManualResetEvent(false);

                string sourcePath = string.Copy(filePath);
                string dir = string.Copy(sourceDirectory);
                string ctr = string.Copy(containerName);

                ThreadPool.QueueUserWorkItem(delegate { ProcessFile(sourcePath, dir, ctr); resetEvent.Set(); });
                uploads.Add(resetEvent);
                started++;
                Console.WriteLine(uploads.Count + " uploads in progress. " + started + " started.");
            }

            Console.WriteLine("Waiting for all uploads to complete");
            completed += uploads.Count;

            WaitHandle.WaitAll(uploads.ToArray());
            Console.WriteLine("Finished - " + completed + " uploads.");
        }

        public static string RationalisePath(string path)
        {
            path = path.Replace("/", @"\");

            if (!path.EndsWith(@"\"))
                path += @"\";

            return path;
        }

        public static void ProcessFile(string path, string basePath, string containerName)
        {
            long length = new FileInfo(path).Length;
            var originalPath = path;

            path = path.Replace(basePath, "");
            Console.WriteLine(length + " " + path);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            container.CreateIfNotExists();

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(path);
            
            var upload = true;

            if (blockBlob.Exists())
            {
                blockBlob.FetchAttributes();
                var size = blockBlob.Properties.Length;

                if (size == length)
                {
                    upload = false;
                    Console.WriteLine("Skip "  + path +  " as it exists.");
                }
            }

            if (upload)
            {
                Console.WriteLine("Uploading " + path);
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                using (var fileStream = System.IO.File.OpenRead(originalPath))
                {
                    blockBlob.UploadFromStream(fileStream);
                }
                stopWatch.Stop();

                // Get the elapsed time as a TimeSpan value.
                TimeSpan ts = stopWatch.Elapsed;

                // Format and display the TimeSpan value.
                string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                    ts.Hours, ts.Minutes, ts.Seconds,
                    ts.Milliseconds / 10);

                Console.WriteLine("Uploaded " + path + " " + elapsedTime);
            }
        }
    }
}
