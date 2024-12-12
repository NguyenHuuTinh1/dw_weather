using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class FactTable
{
    public int Id { get; set; }

    public int CountryId { get; set; }

    public int? LocationId { get; set; }

    public int? WeatherId { get; set; }

    public int? DateId { get; set; }

    public int? ReportId { get; set; }

    public double? Temperature { get; set; }

    public double? Visibility { get; set; }

    public double? Pressure { get; set; }

    public int? Humidity { get; set; }

    public double? DewPoint { get; set; }

    public DateTime? DeadTime { get; set; }
}
