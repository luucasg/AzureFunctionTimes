using AzureFunctionsTimes.Common.Responses;
using AzureFunctionsTimes.Functions.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureFunctionsTimes.Functions.Functions
{
    public static class ConsolidatedApi
    {
        [FunctionName(nameof(Consolidated))]
        public static async Task<IActionResult> Consolidated(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "consolidated")] HttpRequest req,
            [Table("time", Connection = "AzureWebJobsStorage")] CloudTable timeTable,
            [Table("consolidated", Connection = "AzureWebJobsStorage")] CloudTable consolidatedTable,
            ILogger log)
        {
            log.LogInformation("Get all consolidated received.");

            string notConsolidatedFilter = TableQuery.GenerateFilterConditionForBool("IsConsolidated", QueryComparisons.Equal, false);
            TableQuery<TimeEntity> query = new TableQuery<TimeEntity>().Where(notConsolidatedFilter);
            TableQuerySegment<TimeEntity> timesWithoutConsolidated = await timeTable.ExecuteQuerySegmentedAsync(query, null);
            int totalAdd = 0;
            int totalUpdate = 0;

            List<IGrouping<int, TimeEntity>> groupedTimes = (from t in timesWithoutConsolidated group t by t.EmployeeId).ToList();
            foreach (IGrouping<int, TimeEntity> groupTime in groupedTimes)
            {
                TimeSpan difference;
                double totalMinutes = 0;
                List<TimeEntity> orderedTimes = groupTime.OrderBy(x => x.Date).ToList();
                int pair = orderedTimes.Count % 2 == 0 ? orderedTimes.Count : orderedTimes.Count - 1;
                TimeEntity[] auxTimes = orderedTimes.ToArray();
                try
                {
                    for (int i = 0; i < pair; i++)
                    {
                        await SetIsConsolidatedAsync(auxTimes[i].RowKey, timeTable);
                        if (i % 2 != 0 && auxTimes.Length > 1)
                        {
                            difference = auxTimes[i].Date - auxTimes[i - 1].Date;
                            totalMinutes += difference.TotalMinutes;
                            TableQuery<ConsolidatedEntity> consolidatedQuery = new TableQuery<ConsolidatedEntity>();
                            TableQuerySegment<ConsolidatedEntity> allConsolidated = await consolidatedTable.ExecuteQuerySegmentedAsync(consolidatedQuery, null);
                            IEnumerable<ConsolidatedEntity> findConsolidatedResult = allConsolidated.Where(x => x.EmployeeId == auxTimes[i].EmployeeId);
                            if (findConsolidatedResult == null || findConsolidatedResult.Count() == 0)
                            {
                                ConsolidatedEntity consolidatedEntity = new ConsolidatedEntity
                                {
                                    EmployeeId = auxTimes[i].EmployeeId,
                                    Date = auxTimes[i].Date,
                                    WorkedMinutes = (int)totalMinutes,
                                    ETag = "*",
                                    PartitionKey = "CONSOLIDATED",
                                    RowKey = auxTimes[i].RowKey
                                };
                                TableOperation addConsolidatedOperation = TableOperation.Insert(consolidatedEntity);
                                await consolidatedTable.ExecuteAsync(addConsolidatedOperation);
                                totalAdd++;
                                totalMinutes = 0;
                            }
                            else
                            {
                                TableOperation findOp = TableOperation.Retrieve<ConsolidatedEntity>("CONSOLIDATED", findConsolidatedResult.First().RowKey);
                                TableResult findRes = await consolidatedTable.ExecuteAsync(findOp);
                                ConsolidatedEntity consolidatedEntity = (ConsolidatedEntity)findRes.Result;
                                consolidatedEntity.WorkedMinutes += (int)totalMinutes;
                                consolidatedEntity.Date = findConsolidatedResult.First().Date;
                                TableOperation addConsolidatedOperation = TableOperation.Replace(consolidatedEntity);
                                await consolidatedTable.ExecuteAsync(addConsolidatedOperation);
                                totalUpdate++;
                                totalMinutes = 0;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    string errorMessage = e.Message;
                    throw;
                }
            }
            string message = $"Consolidation summary. Records added: {totalAdd}, records updated: {totalUpdate}.";
            log.LogInformation(message);

            return new OkObjectResult(new Response
            {
                IsSuccess = true,
                Message = message,
                Result = null
            });
        }

        private static async Task SetIsConsolidatedAsync(string id, CloudTable timeTable)
        {
            TableOperation findOperation = TableOperation.Retrieve<TimeEntity>("TIME", id);
            TableResult findResult = await timeTable.ExecuteAsync(findOperation);
            TimeEntity timeEntity = (TimeEntity)findResult.Result;
            timeEntity.IsConsolidated = true;
            TableOperation addOperation = TableOperation.Replace(timeEntity);
            await timeTable.ExecuteAsync(addOperation);
        }

        [FunctionName(nameof(GetConsolidatedByDate))]
        public static async Task<IActionResult> GetConsolidatedByDate(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "consolidate/{parameterDate}")] HttpRequest req,
           [Table("consolidated", Connection = "AzureWebJobsStorage")] CloudTable consolidatedTable,
           string parameterDate,
           ILogger log)
        {
            log.LogInformation($"Getting all the consolidates for day {parameterDate}.");

            DateTime date = DateTime.Parse(parameterDate);
            DateTime finalDate = DateTime.Parse(parameterDate + " 23:59 ");

            TableQuery<ConsolidatedEntity> consolidatedQuery = new TableQuery<ConsolidatedEntity>();
            TableQuerySegment<ConsolidatedEntity> allConsolidated = await consolidatedTable.ExecuteQuerySegmentedAsync(consolidatedQuery, null);
            List<ConsolidatedEntity> consolidatedInRange = new List<ConsolidatedEntity>();

            foreach (ConsolidatedEntity consolidated in allConsolidated)
            {
                if (consolidated.Date >= date && consolidated.Date <= finalDate)
                {
                    consolidatedInRange.Add(consolidated);
                }
            }

            string message = consolidatedInRange.Count() > 0 ? 
                $"Consolidates for date: {parameterDate} retrieved" : "There aren't consolidates to show in this date.";
            log.LogInformation(message);

            return new OkObjectResult(new Response
            {
                IsSuccess = true,
                Message = message,
                Result = consolidatedInRange
            });
        }
    }
}
