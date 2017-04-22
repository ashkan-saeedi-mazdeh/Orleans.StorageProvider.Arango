using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using Orleans.Serialization;
using System.Threading.Tasks;
using Orleans.Providers;
using Orleans.Runtime;
using ArangoDB.Client;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Orleans.StorageProvider.Arango
{
    public class ArangoStorageProvider : IStorageProvider
    {
        public static ArangoDatabase Database { get; private set; }
        public Logger Log { get; private set; }
        public string Name { get; private set; }
        private List<ArangoDB.Client.Data.CreateCollectionResult> collectionsList;
        private bool waitForSync;
        private JsonSerializer settings;
        private static bool isInitialized = false;

        public Task Close()
        {
            Database.Dispose();
            return TaskDone.Done;
        }

        public async Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Log = providerRuntime.GetLogger(nameof(ArangoStorageProvider));
            this.Name = name;

            var databaseName = config.GetProperty("DatabaseName", "Orleans");
            var url = config.GetProperty("Url", "http://localhost:8529");
            var username = config.GetProperty("Username", "root");
            var password = config.GetProperty("Password", "password");
            waitForSync = config.GetBoolProperty("WaitForSync", true);

            var grainRefConverter = new GrainReferenceConverter();

            settings = new JsonSerializer();
            settings.DefaultValueHandling = DefaultValueHandling.Include;
            settings.MissingMemberHandling = MissingMemberHandling.Ignore;
            settings.ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor;
            settings.Converters.Add(grainRefConverter);

            if (!isInitialized)
            {
                ArangoDatabase.ChangeSetting(s =>
                {
                    s.Database = databaseName;
                    s.Url = url;
                    s.Credential = new NetworkCredential(username, password);
                    s.DisableChangeTracking = true;
                    s.WaitForSync = waitForSync;
                    s.Serialization.Converters.Add(grainRefConverter);
                });
                isInitialized = true;
            }
            Database = new ArangoDatabase();
            collectionsList = await Database.ListCollectionsAsync();
        }

        private async Task CreateCollectionIfNeeded(bool waitForSync, string collectionName)
        {
            try
            {

                if (!collectionsList.Any(x => x.Name == collectionName))
                {
                    var addedCollection = await Database.CreateCollectionAsync(collectionName, waitForSync: waitForSync);
                    collectionsList.Add(addedCollection);
                }
            }
            catch (Exception ex)
            {
                this.Log.Info($"Arango Storage Provider: Error creating {collectionName} collection, it may already exist");
            }
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string collectionName = ConvertGrainTypeToCollectionName(grainType);
                await CreateCollectionIfNeeded(this.waitForSync, collectionName);
                string primaryKey = ConvertGrainReferenceToDocumentKey(grainReference);
                if (Log.IsVerbose)
                    Log.Info("reading {0}", primaryKey);


                var result = await Database.Collection(collectionName).DocumentAsync<GrainState>(primaryKey).ConfigureAwait(false);
                Log.Info("is it null {0}", result == null);
                if (null == result)
                {
                    return;
                }

                if (result.State != null)
                {
                    grainState.State = (result.State as JObject).ToObject(grainState.State.GetType(), settings);
                }
                else
                {
                    grainState.State = null;
                }
                Log.Info("Reading result {0} eTag:{1}", result.Id, result.Revision);
                grainState.ETag = result.Revision;
            }
            catch (Exception ex)
            {
                this.Log.Error(190000, "ArangoStorageProvider.ClearStateAsync()", ex);
                throw new ArangoStorageException(ex.ToString());
            }
        }

        private string ConvertGrainReferenceToDocumentKey(GrainReference grainReference)
        {
            var primaryKey = grainReference.ToKeyString();

            primaryKey = primaryKey.Replace("GrainReference=", "GR:").Replace("+", "_");
            return primaryKey;
        }

        private string ConvertGrainTypeToCollectionName(string grainType)
        {

            var index = grainType.LastIndexOf(".");

            string collectionName;
            if (index < 0)
                collectionName = grainType;
            else
                collectionName = grainType.Substring(index + 1, grainType.Length - (index + 1));
            return collectionName;
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string collectionName = ConvertGrainTypeToCollectionName(grainType);
                await CreateCollectionIfNeeded(this.waitForSync, collectionName);
                string primaryKey = ConvertGrainReferenceToDocumentKey(grainReference);
                var document = new GrainState
                {
                    Id = primaryKey,
                    Revision = grainState.ETag,
                    State = grainState.State  //JsonConvert.SerializeObject(grainState.State, grainState.State.GetType(), settings)
                };
                if (Log.IsVerbose)
                    Log.Info("writing {0} with type {1} and eTag {2}", primaryKey, grainType, grainState.ETag);

                if (string.IsNullOrWhiteSpace(grainState.ETag))
                {
                    var result = await Database.Collection(collectionName).InsertAsync(document).ConfigureAwait(false);
                    grainState.ETag = result.Rev;
                }
                else
                {
                    var result = await Database.Collection(collectionName).UpdateByIdAsync(primaryKey, document).ConfigureAwait(false);
                    grainState.ETag = result.Rev;
                }
            }
            catch (Exception ex)
            {
                this.Log.Error(190001, "ArangoStorageProvider.WriteStateAsync()", ex);
                throw new ArangoStorageException(ex.ToString());
            }
        }

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                string collectionName = ConvertGrainTypeToCollectionName(grainType);
                await CreateCollectionIfNeeded(this.waitForSync, collectionName);
                string primaryKey = ConvertGrainReferenceToDocumentKey(grainReference);

                await Database.Collection(collectionName).RemoveByIdAsync(primaryKey).ConfigureAwait(false);
                grainState.ETag = null;
            }
            catch (Exception ex)
            {
                this.Log.Error(190002, "ArangoStorageProvider.ClearStateAsync()", ex);
                throw new ArangoStorageException(ex.ToString());
            }
        }
    }

    internal class GrainReferenceInfo
    {
        public string Key { get; set; }

        public string Data { get; set; }
    }

    internal class GrainReferenceConverter : JsonConverter
    {

        static JsonSerializerSettings serializerSettings = OrleansJsonSerializer.GetDefaultSerializerSettings();

        public override bool CanRead
        {
            get { return true; }
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var reference = (GrainReference)value;
            string key = reference.ToKeyString();
            var info = new GrainReferenceInfo
            {
                Key = key,
                Data = JsonConvert.SerializeObject(value, serializerSettings)
            };
            serializer.Serialize(writer, info);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var info = new GrainReferenceInfo();

            serializer.Populate(reader, info);

            return JsonConvert.DeserializeObject(info.Data, serializerSettings);
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(IGrain).IsAssignableFrom(objectType);
        }
    }
}
