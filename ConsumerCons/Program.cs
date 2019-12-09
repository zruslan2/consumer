using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ConsumerCons
{
    public class FileChunk
    {
        public string Hash { get; set; }
        public byte[] Content { get; set; }
        public int ChunkN { get; set; }
        public string FileName { get; set; }
        public int ChunkCount { get; set; }
    }

    public class Receive
    {
        public List<FileChunk> chunks { get; set; } = new List<FileChunk>();

        public void ReceiveF()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "files",
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);

                    var fc = JsonConvert.DeserializeObject<FileChunk>(message);
                    chunks.Add(fc);

                    Console.WriteLine(" [x] Received {0}");
                };
                channel.BasicConsume(queue: "files",
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        public void CreatFile()
        {
            byte[] bytes = new byte[0];
            List<string> files = new List<string>();           

            var f = chunks.GroupBy(p => p.FileName).ToList();

            foreach (var item in f)
            {
                files.Add(item.Key);
            }

            foreach (var item in files)
            {
                if (chunks.Count(p => p.FileName == item) == chunks.FirstOrDefault(c => c.FileName == item).ChunkCount)
                {
                    List<byte> lb = new List<byte>();
                    foreach (var chunk in chunks)
                    {
                        if (chunk.FileName == item)
                            lb.AddRange(chunk.Content);
                    }
                    string basePath = @"F:\";
                    string path = basePath + item;
                    bytes = lb.ToArray();

                    using (var fs = new FileStream(path, FileMode.OpenOrCreate))
                    {
                        fs.Write(bytes, 0, bytes.Length);
                    }

                    string hash = ComputeMD5Checksum(path);

                    if (hash.Equals(chunks.FirstOrDefault(c => c.FileName == item).Hash))
                        Console.WriteLine("Файл верный");
                    else
                        Console.WriteLine("Файл не верный");
                }
            }
            
            Console.ReadLine();
        }

        private string ComputeMD5Checksum(string path)
        {
            using (FileStream fs = System.IO.File.OpenRead(path))
            {
                MD5 md5 = new MD5CryptoServiceProvider();
                byte[] fileData = new byte[fs.Length];
                fs.Read(fileData, 0, (int)fs.Length);
                byte[] checkSum = md5.ComputeHash(fileData);
                string result = BitConverter.ToString(checkSum).Replace("-", String.Empty);
                return result;
            }
        }

        class Program
        {
            static void Main(string[] args)
            {
                Receive r = new Receive();
                r.ReceiveF();
                r.CreatFile();
            }
        }
    }
}
