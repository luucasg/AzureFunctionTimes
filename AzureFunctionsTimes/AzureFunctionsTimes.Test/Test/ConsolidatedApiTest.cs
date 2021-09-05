using AzureFunctionsTimes.Functions.Functions;
using AzureFunctionsTimes.Test.Helpers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Internal;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using Xunit;

namespace AzureFunctionsTimes.Test.Test
{
    public class ConsolidatedApiTest
    {
        [Fact]
        public async void Consolidated_Should_Log_Message()
        {
            // Arrange
            MockCloudTableTimes mockTimes = new MockCloudTableTimes(new Uri("http://127.0.0.1:10002/devstoreaccount1/reports"));
            MockCloudTableConsolidated mockConsolidated = new MockCloudTableConsolidated(new Uri("http://127.0.0.1:10002/devstoreaccount1/reports"));
            ListLogger logger = (ListLogger)TestFactory.CreateLogger(LoggerTypes.List);

            // Add
            IActionResult response = await ConsolidatedApi.Consolidated(null, mockTimes, mockConsolidated, logger);
            string message = logger.Logs[0];

            // Assert
            Assert.Contains("Get all consolidated received.", message);
        }

        [Fact]
        public async Task GetConsolidatedByDate_Should_Return_200()
        {
            // Arrenge
            MockCloudTableConsolidated mockConsolidated = new MockCloudTableConsolidated(new Uri("http://127.0.0.1:10002/devstoreaccount1/reports"));
            ILogger logger = TestFactory.CreateLogger();
            string date = "2021/09/05";
            DefaultHttpRequest request = TestFactory.CreateHttpRequest(date);

            // Act
            IActionResult response = await ConsolidatedApi.GetConsolidatedByDate(request, mockConsolidated, date, logger);

            // Assert
            OkObjectResult result = (OkObjectResult)response;
            Assert.Equal(StatusCodes.Status200OK, result.StatusCode);
        }
    }
}
