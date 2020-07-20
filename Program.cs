using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;

namespace jsonToParquet
{
    public class Program
    {
        private static string IN_PATH = @"Data\";
        private static string OUT_PATH = @"parquet\";
        static void Main(string[] args)
        {
            GenerateParquetFilesFromDirectory(IN_PATH, OUT_PATH);
            Console.ReadLine();
        }

        public static void GenerateParquetFilesFromDirectory(string inPath, string outPath)
        {
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), inPath);
            foreach (string file in Directory.EnumerateFiles(path, "*.json"))
            {
                var result = Path.ChangeExtension(file, ".parquet");
                var licensePlateList = new List<string>();
                var sensorList = new List<int>();
                var timeList = new List<string>();
                string[] jsonLines = File.ReadAllLines(file);
                foreach (string json in jsonLines)
                {
                    var carData = JsonConvert.DeserializeObject<CarData>(json);
                    licensePlateList.Add(carData.LicensePlate);
                    sensorList.Add(carData.Sensor);
                    timeList.Add(carData.Time);
                }
                var licensePlateColumn = new DataColumn(
                new DataField<string>("LicensePlate"),
                licensePlateList.ToArray());

                var sensorColumn = new DataColumn(
                   new DataField<int>("Sensor"),
                   sensorList.ToArray());

                var timeColumn = new DataColumn(
                   new DataField<string>("Time"),
                   timeList.ToArray());

                var outPutPath = outPath + Path.GetFileName(Path.ChangeExtension(file, ".parquet"));
                BuildParquetFile(licensePlateColumn, sensorColumn, timeColumn, outPutPath);
            }
            Console.WriteLine("Done converting JSONL files to Parquet!");
        }

        public static void BuildParquetFile(DataColumn license, DataColumn sensor, DataColumn time, string outPath)
        {
            var schema = new Schema(license.Field, sensor.Field, time.Field);
            using (Stream fileStream = File.Create(outPath))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {
                    parquetWriter.CompressionMethod = CompressionMethod.Gzip;
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(license);
                        groupWriter.WriteColumn(sensor);
                        groupWriter.WriteColumn(time);
                    }
                }
            }
        }

        public class CarData
        {
            [JsonProperty("LicensePlate")]
            public string LicensePlate { get; set; }
            [JsonProperty("Sensor")]
            public int Sensor { get; set; }
            [JsonProperty("Time")]
            public string Time { get; set; }
        }
    }
}
