using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctionsTimes.Common.Models
{
    public class Time
    {
        public int EmployeeId { get; set; }
        public DateTime Date { get; set; }
        public int Type { get; set; }
        public bool IsConsolidated { get; set; }
    }
}
