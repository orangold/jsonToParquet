using Parquet.Data;
using System;
using System.Collections.Generic;
using static jsonToParquet.Program;

namespace jsonToParquet
{
    public static class DummyParquetFileGenerator
    {
        static Random rd = new Random();
        public static int NUM_OF_CARS = 10000000;

        public static void GenerateGiantParquetFile()
        {
            var licensePlateList = new List<string>();
            var sensorList = new List<int>();
            var timeList = new List<string>();

            for (int i = 0; i < NUM_OF_CARS; i++)
            {
                var carData = CreateRandomCarData();
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

            BuildParquetFile(licensePlateColumn, sensorColumn, timeColumn, @"bigParquet/dummyParquet.parquet");
            Console.WriteLine("Done Generating huge parquet file!");
        }

        public static CarData CreateRandomCarData()
        {
            var carData = new CarData();
            carData.LicensePlate = CreateRandomLicensePlate(10);
            carData.Sensor = GetRandomSensorId();
            carData.Time = GetRandomDateTime(new DateTime(2002, 7, 31), new DateTime(2002, 7, 1));
            return carData;
        }

        public static string CreateRandomLicensePlate(int stringLength)
        {
            const string allowedChars = "ABCDEFGHJKLMNOPQRSTUVWXYZ0123456789";
            char[] chars = new char[stringLength];
            for (int i = 0; i < stringLength; i++)
            {
                chars[i] = allowedChars[rd.Next(0, allowedChars.Length)];
            }
            return new string(chars);
        }

        public static string GetRandomDateTime(DateTime endDate, DateTime startDate)
        {
            var randomTest = new Random();
            TimeSpan timeSpan = endDate - startDate;
            TimeSpan newSpan = new TimeSpan(0, randomTest.Next(0, (int)timeSpan.TotalMinutes), 0);
            var date = startDate + newSpan;
            return date.ToString("yyyy-MM-dd HH:mm:ss");
        }

        public static int GetRandomSensorId()
        {
            return rd.Next(1, 100);
        }
    }
}
