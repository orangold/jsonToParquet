using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Parquet;
using Parquet.Data;

namespace jsonToParquet
{
    class Program
    {
        static void Main(string[] args)
        {
            GenerateParquetFile();
            Console.ReadLine();
        }


        public static void GenerateParquetFile()
        {
            int fileSuffix = 1;
            string path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\");
            List<string> licensePlateList = new List<string>();
            List<int> sensorList = new List<int>();
            List<string> timeList = new List<string>();

            foreach (string file in Directory.EnumerateFiles(path, "*.json"))
            {
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

                var schema = new Schema(licensePlateColumn.Field, sensorColumn.Field, timeColumn.Field);

                using (Stream fileStream = System.IO.File.Create("parquet\\carData" + fileSuffix + ".parquet"))
                {
                    using (var parquetWriter = new ParquetWriter(schema, fileStream))
                    {
                        // create a new row group in the file
                        using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                        {
                            groupWriter.WriteColumn(licensePlateColumn);
                            groupWriter.WriteColumn(sensorColumn);
                            groupWriter.WriteColumn(timeColumn);
                        }
                    }
                }
                fileSuffix++;
            }

            Console.WriteLine("Done!");
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
