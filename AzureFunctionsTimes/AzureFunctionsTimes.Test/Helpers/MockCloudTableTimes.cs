using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace AzureFunctionsTimes.Test.Helpers
{
    public class MockCloudTableTimes : CloudTable
    {
        public MockCloudTableTimes(Uri tableAddress) : base(tableAddress)
        {
        }

        public MockCloudTableTimes(Uri tableAbsoluteUri, StorageCredentials credentials) : base(tableAbsoluteUri, credentials)
        {
        }

        public MockCloudTableTimes(StorageUri tableAddress, StorageCredentials credentials) : base(tableAddress, credentials)
        {
        }

        public override async Task<TableResult> ExecuteAsync(TableOperation operation)
        {
            return await Task.FromResult(new TableResult
            {
                HttpStatusCode = 200,
                Result = TestFactory.GetTimeEntity()
            });
        }

        public override async Task<TableQuerySegment<WatchEntity>> ExecuteQuerySegmentedAsync<WatchEntity>(TableQuery<WatchEntity> query, TableContinuationToken token)
        {
            ConstructorInfo constructor = typeof(TableQuerySegment<WatchEntity>)
                   .GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
                   .FirstOrDefault(c => c.GetParameters().Count() == 1);

            return await Task.FromResult(constructor.Invoke(new object[] { TestFactory.GetAllTimesRequest() }) as TableQuerySegment<WatchEntity>);
        }
    }
}
