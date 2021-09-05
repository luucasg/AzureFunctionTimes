using AzureFunctionsTimes.Functions.Entities;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureFunctionsTimes.Functions.Functions
{
    public static class ScheduledFunction
    {
        [FunctionName("ScheduledFunction")]
        public static async Task Run(
            [TimerTrigger("0 */60 * * * *")] TimerInfo myTimer,
            [Table("time", Connection = "AzureWebJobsStorage")] CloudTable timeTable,
            [Table("consolidated", Connection = "AzureWebJobsStorage")] CloudTable consolidatedTable,
            ILogger log)
        {
            log.LogInformation("Get all consolidated received.");

            string notConsolidatedFilter = TableQuery.GenerateFilterConditionForBool("IsConsolidated", QueryComparisons.Equal, false);
            TableQuery<TimeEntity> query = new TableQuery<TimeEntity>().Where(notConsolidatedFilter);
            TableQuerySegment<TimeEntity> timesWithoudConsolidated = await timeTable.ExecuteQuerySegmentedAsync(query, null);
            int totalAdd = 0;
            int totalUpdate = 0;
            var groupedTimes = (from t in timesWithoudConsolidated group t by t.EmployeeId).ToList();
            foreach (var groupTime in groupedTimes)
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
                            var findConsolidatedResult = allConsolidated.Where(x => x.EmployeeId == auxTimes[i].EmployeeId);
                            if (findConsolidatedResult == null || findConsolidatedResult.Count() == 0)
                            {
                                ConsolidatedEntity consolidatedEntity = new ConsolidatedEntity
                                {
                                    EmployeeId = auxTimes[i].EmployeeId,
                                    Date = DateTime.Today,
                                    WorkedMinutes = (int)totalMinutes,
                                    ETag = "*",
                                    PartitionKey = "CONSOLIDATED",
                                    RowKey = auxTimes[i].RowKey
                                };
                                TableOperation addConsolidatedOperation = TableOperation.Insert(consolidatedEntity);
                                await consolidatedTable.ExecuteAsync(addConsolidatedOperation);
                                totalAdd++;
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
            log.LogInformation($"Consolidation summary. Records added: {totalAdd}, records updated: {totalUpdate}.");
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
    }
}
