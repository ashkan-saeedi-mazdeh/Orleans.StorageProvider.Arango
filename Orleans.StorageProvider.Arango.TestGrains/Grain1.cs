﻿using Orleans.Providers;
using System;
using System.Threading.Tasks;

namespace Orleans.StorageProvider.Arango.TestGrains
{
    public interface IGrain1 : IGrainWithIntegerKey
    {
        Task Set(string stringValue, int intValue, DateTime dateTimeValue, Guid guidValue);
        Task<Tuple<string, int, DateTime, Guid>> Get();
        Task Clear();
    }

    public class MyState 
    {
        public string StringValue { get; set; }
        public int IntValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
    }

    [StorageProvider(ProviderName = "ARANGO")]
    public class Grain1 : Grain<MyState>, IGrain1
    {
        public Task Set(string stringValue, int intValue, DateTime dateTimeValue, Guid guidValue)
        {
            try
            {
                State.StringValue = stringValue;
                State.IntValue = intValue;
                State.DateTimeValue = dateTimeValue;
                State.GuidValue = guidValue;
                return WriteStateAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }


        }

        public Task<Tuple<string, int, DateTime, Guid>> Get()
        {
            try
            {
                this.ReadStateAsync();

                return Task.FromResult(new Tuple<string, int, DateTime, Guid>(
                  State.StringValue,
                  State.IntValue,
                  State.DateTimeValue,
                  State.GuidValue));

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }

        public async Task Clear()
        {
            try
            {
                await ClearStateAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }
    }
}
